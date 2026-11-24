"""Real processes, real SIGKILLs: every transfer finishes and the bank's
balances match what the workflow results say."""

from durable.chaos import run


def test_kill_9_workers():
    res = run(transfers=80, workers=3, kill_every=0.2, seed=1, timeout=120)
    assert res["kills"] >= 10
    assert not res["problems"], res["problems"][:5]
    assert sum(res["statuses"].values()) == 80
