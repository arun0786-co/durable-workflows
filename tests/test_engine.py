import asyncio

import pytest

from durable import ActivityError, Cancelled, ChildError, RetryPolicy, WorkflowFailed, activity_context

calls = []


def add(a, b):
    calls.append(("add", a, b))
    return a + b


def flaky(n):
    """Fails the first n attempts."""
    if activity_context().attempt <= n:
        raise ConnectionError(f"attempt {activity_context().attempt}")
    return activity_context().attempt


def broken(x):
    raise ValueError(f"bad input {x}")


def record(x):
    calls.append(("record", x))
    return x


async def adds(ctx, n):
    total = 0
    for i in range(n):
        total = await ctx.activity(add, total, i)
    return total


async def fan_out(ctx, n):
    return await ctx.gather(*[ctx.activity(add, i, i) for i in range(n)])


async def retrying(ctx):
    return await ctx.activity(flaky, 2, retry=RetryPolicy(max_attempts=5, initial=10, backoff=3))


async def compensating(ctx, x):
    try:
        await ctx.activity(broken, x, retry=RetryPolicy(non_retryable=["ValueError"]))
    except ActivityError as e:
        await ctx.activity(record, f"undo {x}")
        return f"compensated after {e.type}"


async def sleeper(ctx, secs):
    t0 = ctx.now()
    await ctx.sleep(secs)
    return ctx.now() - t0


async def approval(ctx, deadline):
    i, v = await ctx.first(ctx.wait_signal("approve"), ctx.sleep(deadline))
    return v if i == 0 else "timed out"


async def child_adds(ctx, n):
    return await ctx.child(adds, n)


async def child_fails(ctx):
    try:
        await ctx.child(compensating_fails)
    except ChildError as e:
        return e.type


async def compensating_fails(ctx):
    raise KeyError("nope")


async def cancellable(ctx):
    await ctx.activity(record, "reserve")
    try:
        await ctx.sleep(10**6)
    except Cancelled:
        await ctx.activity(record, "release")
        raise


async def counter(ctx, total, done):
    """Adds up 'add' signals; continues as new every 3 signals."""
    for _ in range(3):
        total += await ctx.wait_signal("add")
        done += 1
        if done == 7:
            return total
    late = ctx.drain_signals("add")
    total, done = total + sum(late), done + len(late)
    if done == 7:
        return total
    ctx.continue_as_new(total, done)


async def uses_asyncio(ctx):
    await asyncio.sleep(0)


def test_sequential_activities(env_factory):
    env = env_factory([adds], [add])
    wid = env.client.start(adds, 5)
    env.run()
    assert env.client.result(wid) == 0 + 1 + 2 + 3 + 4
    # one workflow task per activity result, plus the first
    assert sum(e["type"] == "task_done" for e in env.client.history(wid)) == 6


def test_parallel_activities_scheduled_in_one_task(env_factory):
    env = env_factory([fan_out], [add])
    wid = env.client.start(fan_out, 4)
    env.run()
    assert env.client.result(wid) == [0, 2, 4, 6]
    h = env.client.history(wid)
    assert [e["type"] for e in h[:6]] == ["started", "task_done"] + ["activity_scheduled"] * 4


def test_retry_with_backoff(env_factory):
    env = env_factory([retrying], [flaky])
    wid = env.client.start(retrying)
    t0 = env.clock.t
    env.run()
    assert env.client.result(wid) == 3
    assert env.clock.t - t0 == 10 + 30  # waits after attempt 1 and 2


def test_non_retryable_error_and_compensation(env_factory):
    calls.clear()
    env = env_factory([compensating], [broken, record])
    wid = env.client.start(compensating, 7)
    env.run()
    assert env.client.result(wid) == "compensated after ValueError"
    assert calls == [("record", "undo 7")]


def test_durable_timer(env_factory):
    env = env_factory([sleeper])
    wid = env.client.start(sleeper, 3600)
    env.drain()
    env.clock.t += 3599
    env.drain()
    assert env.client.describe(wid)["status"] == "running"
    env.clock.t += 1
    env.drain()
    assert env.client.result(wid) == 3600


@pytest.mark.parametrize("signal_at, expect", [(50, "yes"), (150, "timed out")])
def test_signal_or_timeout(env_factory, signal_at, expect):
    env = env_factory([approval])
    wid = env.client.start(approval, 100)
    env.drain()
    env.clock.t += signal_at
    if signal_at < 100:
        env.client.signal(wid, "approve", "yes")
    env.run()
    assert env.client.result(wid) == expect


def test_child_workflow(env_factory):
    env = env_factory([child_adds, adds], [add])
    wid = env.client.start(child_adds, 4)
    env.run()
    assert env.client.result(wid) == 6
    assert env.client.describe(f"{env.client.describe(wid)['run_id']}/1")["status"] == "completed"


def test_child_failure(env_factory):
    env = env_factory([child_fails, compensating_fails])
    wid = env.client.start(child_fails)
    env.run()
    assert env.client.result(wid) == "KeyError"


def test_cancel_runs_compensation(env_factory):
    calls.clear()
    env = env_factory([cancellable], [record])
    wid = env.client.start(cancellable)
    env.drain()
    env.client.cancel(wid)
    env.drain()
    assert env.client.describe(wid)["status"] == "cancelled"
    assert calls == [("record", "reserve"), ("record", "release")]
    with pytest.raises(WorkflowFailed):
        env.client.result(wid)


def test_continue_as_new_keeps_signals(env_factory):
    env = env_factory([counter])
    wid = env.client.start(counter, 0, 0)
    env.drain()
    for i in range(1, 8):
        env.client.signal(wid, "add", i)
        if i % 2:
            env.drain()
    env.run()
    assert env.client.result(wid) == sum(range(1, 8))
    statuses = [r["status"] for r in env.store.runs() if r["workflow_id"] == wid]
    assert statuses[-1] == "completed" and "continued" in statuses


def test_idempotent_start(env_factory):
    env = env_factory([adds], [add])
    a = env.client.start(adds, 3, id="order-1")
    b = env.client.start(adds, 3, id="order-1")
    assert a == b and len(env.store.runs()) == 1


def test_awaiting_asyncio_fails_the_workflow(env_factory):
    env = env_factory([uses_asyncio])
    wid = env.client.start(uses_asyncio)
    env.run()
    d = env.client.describe(wid)
    assert d["status"] == "failed" and d["result"]["type"] == "TypeError"


def test_expired_lease_retries_and_rejects_the_late_result(env_factory):
    calls.clear()
    env = env_factory([adds], [add])
    wid = env.client.start(adds, 1)
    env.drain_once = env.worker.workflow_step()
    first = env.worker.take_activity()  # this worker then hangs
    env.clock.t += 61  # past the 60 s default lease
    second = env.worker.take_activity()
    assert second.attempt == 2 and second.token != first.token
    assert env.worker.report_activity(second, env.worker.run_activity(second))
    assert not env.worker.report_activity(first, env.worker.run_activity(first))
    env.run()
    assert env.client.result(wid) == 0


def test_events_during_a_task_force_a_rerun(env_factory):
    env = env_factory([approval])
    wid = env.client.start(approval, 100)
    task = env.worker.take_workflow_task()
    out = env.worker.run_workflow_task(task)
    env.drain()  # nothing: the task is leased
    env.client.signal(wid, "approve", "yes")
    assert env.worker.report_workflow_task(task, out) == "conflict"
    env.run()
    assert env.client.result(wid) == "yes"
    assert [e["type"] for e in env.client.history(wid)][:3] == ["started", "signal", "task_done"]


def test_query_replays_state(env_factory):
    async def shopping(ctx):
        cart = []
        ctx.set_query("cart", lambda: list(cart))
        while True:
            item = await ctx.wait_signal("add")
            if item is None:
                return cart
            cart.append(item)

    env = env_factory([shopping])
    wid = env.client.start(shopping)
    for item in ["apple", "pear"]:
        env.client.signal(wid, "add", item)
        env.drain()
    assert env.client.query(shopping, wid, "cart") == ["apple", "pear"]


def test_side_effect_runs_once(env_factory):
    seen = []

    async def wf(ctx):
        v = ctx.side_effect(lambda: seen.append(1) or len(seen))
        await ctx.sleep(1)
        await ctx.sleep(1)
        return v

    env = env_factory([wf])
    wid = env.client.start(wf)
    env.run()
    assert env.client.result(wid) == 1 and len(seen) == 1
