"""Changing workflow code under running workflows: detected when unsafe,
allowed with ctx.patched()."""

from durable import Worker, workflow


def a():
    return "a"


def b():
    return "b"


def c():
    return "c"


@workflow(name="order")
async def v1(ctx):
    return [await ctx.activity(a), await ctx.activity(c)]


@workflow(name="order")
async def v1_reordered(ctx):
    return [await ctx.activity(c), await ctx.activity(a)]


@workflow(name="order")
async def v2_patched(ctx):
    out = [await ctx.activity(a)]
    if ctx.patched("add-b"):
        out.append(await ctx.activity(b))
    out.append(await ctx.activity(c))
    return out


def deploy(env, fn):
    env.worker.shutdown()
    env.worker = Worker(env.store, [fn], [a, b, c], clock=env.clock)


def test_incompatible_change_stops_the_run_until_fixed(env_factory):
    env = env_factory([v1], [a, b, c])
    wid = env.client.start("order")
    env.worker.workflow_step()  # schedules a
    env.worker.activity_step()  # a done; a workflow task is now due
    deploy(env, v1_reordered)
    env.drain()
    d = env.client.describe(wid)
    assert d["status"] == "running"
    assert d["task_error"].startswith("NonDeterminism: the history has activity a() (seq 1)")
    deploy(env, v1)  # roll back
    env.run()
    assert env.client.result(wid) == ["a", "c"]


def test_patch_old_history_keeps_old_path(env_factory):
    env = env_factory([v1], [a, b, c])
    wid = env.client.start("order")
    for _ in range(2):
        env.worker.workflow_step()
        env.worker.activity_step()
    # a and c ran under the old code; the task that would finish the run is due
    deploy(env, v2_patched)
    env.run()
    assert env.client.result(wid) == ["a", "c"]


def test_patch_takes_new_path_from_the_point_reached(env_factory):
    env = env_factory([v1], [a, b, c])
    old = env.client.start("order", id="old")
    env.worker.workflow_step()
    env.worker.activity_step()  # only a has run when the new code arrives
    deploy(env, v2_patched)
    new = env.client.start("order", id="new")
    env.run()
    assert env.client.result(old) == ["a", "b", "c"]
    assert env.client.result(new) == ["a", "b", "c"]


runs = [0]


async def reads_global_state(ctx):
    runs[0] += 1  # differs between the first run and a replay
    first = a if runs[0] % 2 else c
    await ctx.activity(first)
    return await ctx.activity(b)


def test_nondeterministic_code_is_caught(env_factory):
    env = env_factory([reads_global_state], [a, b, c])
    # with the sticky cache the code would not be re-run on this worker; a
    # replay happens on another worker or after eviction, so force one
    env.worker.sticky = False
    wid = env.client.start(reads_global_state)
    env.drain()
    assert "NonDeterminism" in env.client.describe(wid)["task_error"]
