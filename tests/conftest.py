import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from durable import Client, Store, Worker  # noqa: E402


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class Env:
    """An in-memory store, a fake clock, one worker, driven step by step."""

    def __init__(self, workflows=(), activities=()):
        self.clock = Clock()
        self.store = Store()
        self.client = Client(self.store, self.clock)
        self.worker = Worker(self.store, workflows, activities, clock=self.clock)

    def drain(self, limit=10000):
        """Does all the work that is due now."""
        for _ in range(limit):
            w = self.worker
            if not (w.timer_step() | w.workflow_step() | w.activity_step()):
                return
        raise RuntimeError("still busy")

    def run(self, max_time=1e7, rounds=1000):
        """Does all work, advancing the clock to each next due time."""
        for _ in range(rounds):
            self.drain()
            due = self.store.next_due()
            if due is None or due > self.clock.t + max_time:
                return
            self.clock.t = max(self.clock.t, due)


@pytest.fixture
def env_factory():
    return Env
