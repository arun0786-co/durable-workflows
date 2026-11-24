"""Workloads for the simulator. Each one has workflows, activities that act
on an outside system (the "world", which crashes do not roll back), a way
to start them, and invariants that must hold however the run went."""

from .errors import ActivityError
from .policy import RetryPolicy
from .worker import activity_context

FAST = dict(initial=1.0, backoff=2.0, max_interval=8.0)


class Flaky:
    """Fails activity calls on purpose, before or after their side effect."""

    def __init__(self, rng, before=0.0, after=0.0):
        self.rng, self.before, self.after, self.on = rng, before, after, True

    def maybe(self, p, where):
        if self.on and self.rng.random() < p:
            raise ConnectionError(f"injected failure {where} the effect")


# ---- money transfers: a saga with compensation

class Bank:
    """Operations carry a key chosen by the caller. A repeated key returns
    the first result; a voided key is reversed if it was applied and refused
    if it arrives later."""

    def __init__(self, balances, frozen=(), idempotent=True):
        self.balances = dict(balances)
        self.initial = dict(balances)
        self.frozen = set(frozen)
        self.idempotent = idempotent
        self.done = {}  # key -> (account, delta)
        self.voided = set()

    def apply(self, key, account, delta):
        if key in self.voided:
            raise Voided(key)
        if self.idempotent and key in self.done:
            return self.balances[account]
        if account in self.frozen:
            raise Frozen(account)
        if self.balances[account] + delta < 0:
            raise Insufficient(account)
        self.balances[account] += delta
        self.done[key] = (account, delta)
        return self.balances[account]

    def void(self, key):
        if key in self.voided:
            return
        self.voided.add(key)
        if key in self.done:
            account, delta = self.done[key]
            self.balances[account] -= delta


class Frozen(Exception):
    pass


class Insufficient(Exception):
    pass


class Voided(Exception):
    pass


NO_RETRY = ["Frozen", "Insufficient", "Voided"]
FOREVER = dict(max_attempts=10**6, **FAST)


async def transfer(ctx, src, dst, amount):
    """Withdraw, then deposit. A failed step is voided by its key before
    anything else: a call that failed with a timeout or a lost reply may
    still have gone through."""
    retry = RetryPolicy(max_attempts=4, non_retryable=NO_RETRY, **FAST)
    w, d = f"{ctx.workflow_id}:withdraw", f"{ctx.workflow_id}:deposit"
    try:
        await ctx.activity("withdraw", w, src, amount, retry=retry, timeout=5)
    except ActivityError as e:
        await ctx.activity("void", w, retry=RetryPolicy(**FOREVER), timeout=5)
        return {"status": "rejected", "reason": e.type}
    try:
        await ctx.activity("deposit", d, dst, amount, retry=retry, timeout=5)
    except ActivityError as e:
        await ctx.activity("void", d, retry=RetryPolicy(**FOREVER), timeout=5)
        await ctx.activity("void", w, retry=RetryPolicy(**FOREVER), timeout=5)  # the refund
        return {"status": "refunded", "reason": e.type}
    return {"status": "done"}


async def transfer_naive(ctx, src, dst, amount):
    """The first version: treats a step that ran out of retries as a step
    that did not happen. The simulator finds the money it creates."""
    retry = RetryPolicy(max_attempts=4, non_retryable=NO_RETRY, **FAST)
    w, d = f"{ctx.workflow_id}:withdraw", f"{ctx.workflow_id}:deposit"
    try:
        await ctx.activity("withdraw", w, src, amount, retry=retry, timeout=5)
    except ActivityError as e:
        return {"status": "rejected", "reason": e.type}
    try:
        await ctx.activity("deposit", d, dst, amount, retry=retry, timeout=5)
    except ActivityError as e:
        await ctx.activity("refund", f"{ctx.workflow_id}:refund", src, amount, retry=RetryPolicy(**FOREVER), timeout=5)
        return {"status": "refunded", "reason": e.type}
    return {"status": "done"}


class Transfers:
    name = "transfers"
    workflows = [transfer]

    def __init__(self, rng, flaky, n=12, idempotent=True, naive=False):
        accounts = {f"acct{i}": rng.randint(0, 100) for i in range(5)}
        self.bank = Bank(accounts, frozen=[rng.choice(list(accounts))], idempotent=idempotent)
        self.plan = [(f"t{i}", *rng.sample(list(accounts), 2), rng.randint(1, 60)) for i in range(n)]
        self.flaky = flaky
        self.signals = []
        self.wf = transfer_naive if naive else transfer
        self.workflows = [self.wf]

    def activities(self):
        bank, flaky = self.bank, self.flaky

        def call(f, *args):
            flaky.maybe(flaky.before, "before")
            r = f(*args)
            flaky.maybe(flaky.after, "after")
            return r

        def withdraw(key, acct, amount):
            return call(bank.apply, key, acct, -amount)

        def deposit(key, acct, amount):
            return call(bank.apply, key, acct, amount)

        def refund(key, acct, amount):
            return call(bank.apply, key, acct, amount)

        def void(key):
            return call(bank.void, key)

        return [withdraw, deposit, refund, void]

    def start(self, client):
        for wid, src, dst, amount in self.plan:
            client.start(self.wf, src, dst, amount, id=wid)

    def check(self, client, histories):
        bad = []
        expect = dict(self.bank.initial)
        for wid, src, dst, amount in self.plan:
            res = client.describe(wid)["result"]
            if res["status"] == "done":
                expect[src] -= amount
                expect[dst] += amount
        if expect != self.bank.balances:
            diff = {a: (expect[a], self.bank.balances[a]) for a in expect if expect[a] != self.bank.balances[a]}
            bad.append(f"balances differ from what the results imply (expected, actual): {diff}")
        if sum(self.bank.balances.values()) != sum(self.bank.initial.values()):
            bad.append("money was created or destroyed")
        return bad


# ---- approval with a deadline: signal against timer

async def approval(ctx, deadline):
    i, _ = await ctx.first(ctx.wait_signal("approve"), ctx.sleep(deadline))
    if i == 0:
        await ctx.activity("ship", ctx.workflow_id, retry=RetryPolicy(max_attempts=10**6, **FAST), timeout=5)
        return "shipped"
    await ctx.activity("release", ctx.workflow_id, retry=RetryPolicy(max_attempts=10**6, **FAST), timeout=5)
    return "expired"


class Approvals:
    name = "approvals"
    workflows = [approval]

    def __init__(self, rng, flaky, n=10):
        self.flaky = flaky
        self.shipped, self.released = {}, {}
        self.plan = [(f"o{i}", rng.uniform(20, 80)) for i in range(n)]
        # some orders are approved near their deadline, some never
        self.signals = sorted((rng.uniform(0, 100), wid, "approve", True) for wid, _ in self.plan if rng.random() < 0.7)

    def activities(self):
        flaky = self.flaky

        def effect(book):
            def act(order):
                flaky.maybe(flaky.before, "before")
                key = activity_context().idempotency_key
                book.setdefault(order, set()).add(key)
                flaky.maybe(flaky.after, "after")
            return act

        ship, release = effect(self.shipped), effect(self.released)
        ship.__name__, release.__name__ = "ship", "release"
        return [ship, release]

    def start(self, client):
        for wid, deadline in self.plan:
            client.start(approval, deadline, id=wid)

    def check(self, client, histories):
        bad = []
        for wid, _ in self.plan:
            res = client.describe(wid)["result"]
            types = [e["type"] for e in histories[wid][-1]]
            sig = types.index("signal") if "signal" in types else 10**9
            fired = types.index("timer_fired") if "timer_fired" in types else 10**9
            want = "shipped" if sig < fired else "expired"
            if res != want:
                bad.append(f"{wid}: result {res}, but the history says {want}")
            book, other = (self.shipped, self.released) if res == "shipped" else (self.released, self.shipped)
            if len(book.get(wid, ())) != 1 or wid in other:
                bad.append(f"{wid}: {res} with effects shipped={self.shipped.get(wid)} released={self.released.get(wid)}")
        return bad


# ---- a long-lived counter: signals across continue-as-new

async def counter(ctx, total, seen, expected):
    for _ in range(3):
        if seen == expected:
            return total
        total += await ctx.wait_signal("add")
        seen += 1
    late = ctx.drain_signals("add")
    total, seen = total + sum(late), seen + len(late)
    if seen == expected:
        return total
    ctx.continue_as_new(total, seen, expected)


class Counters:
    name = "counters"
    workflows = [counter]

    def __init__(self, rng, flaky, n=3, signals=12):
        self.flaky = flaky
        self.sent = {f"c{i}": [rng.randint(1, 9) for _ in range(signals)] for i in range(n)}
        self.signals = sorted((rng.uniform(0, 60), wid, "add", v) for wid, vs in self.sent.items() for v in vs)

    def activities(self):
        return []

    def start(self, client):
        for wid, vs in self.sent.items():
            client.start(counter, 0, 0, len(vs), id=wid)

    def check(self, client, histories):
        bad = []
        for wid, vs in self.sent.items():
            res = client.describe(wid)["result"]
            if res != sum(vs):
                bad.append(f"{wid}: counted {res}, signals add up to {sum(vs)}")
        return bad


# ---- fan-out over child workflows

async def square(ctx, x):
    return await ctx.activity("square_it", x, retry=RetryPolicy(max_attempts=10**6, **FAST), timeout=5)


async def sum_of_squares(ctx, n):
    return sum(await ctx.gather(*[ctx.child(square, i) for i in range(n)]))


class FanOut:
    name = "fanout"
    workflows = [sum_of_squares, square]

    def __init__(self, rng, flaky, n=3):
        self.flaky = flaky
        self.sizes = {f"f{i}": rng.randint(2, 6) for i in range(n)}
        self.signals = []

    def activities(self):
        flaky = self.flaky

        def square_it(x):
            flaky.maybe(flaky.before, "before")
            return x * x
        return [square_it]

    def start(self, client):
        for wid, n in self.sizes.items():
            client.start(sum_of_squares, n, id=wid)

    def check(self, client, histories):
        return [f"{wid}: {client.describe(wid)['result']} != {sum(i * i for i in range(n))}"
                for wid, n in self.sizes.items() if client.describe(wid)["result"] != sum(i * i for i in range(n))]


SCENARIOS = {s.name: s for s in (Transfers, Approvals, Counters, FanOut)}
