"""The simulator passes on the engine as written, and fails on each
deliberately broken variant: evidence that its checks can see the bugs they
are meant to catch."""

import pytest

from durable.scenarios import SCENARIOS
from durable.sim import simulate


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_seeds_pass(scenario):
    for seed in range(100):
        problems, _, _ = simulate(seed, scenario)
        assert not problems, f"seed {seed}: {problems}"


def test_a_seed_replays_exactly():
    a = simulate(7, "approvals")
    b = simulate(7, "approvals")
    assert a == b


def first_failing(scenario, mutation, limit):
    for seed in range(limit):
        if simulate(seed, scenario, mutation=mutation)[0]:
            return seed
    return None


@pytest.mark.parametrize("scenario, mutation, limit", [
    ("transfers", "naive_saga", 300),      # compensation that trusts "retries ran out"
    ("transfers", "no_idempotency", 50),   # the bank applies a repeated request twice
    ("counters", "skip_event_check", 50),  # commit although events arrived during the task
    ("approvals", "split_commit", 100),    # history and task tables in separate transactions
])
def test_broken_variants_are_caught(scenario, mutation, limit):
    assert first_failing(scenario, mutation, limit) is not None
