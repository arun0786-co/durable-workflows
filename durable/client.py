import time
import uuid

from .runtime import Replayer
from .store import NotFound
from .worker import name_of


class WorkflowFailed(Exception):
    def __init__(self, workflow_id, status, error):
        super().__init__(f"workflow {workflow_id} {status}: {error}")
        self.status = status
        self.error = error


class Client:
    def __init__(self, store, clock=time.time):
        self.store = store
        self.clock = clock

    def start(self, workflow, *args, id=None, if_exists="return"):
        """Starts a workflow and returns its id. With an explicit id the call
        is idempotent: a second start returns the same workflow."""
        wid = id or uuid.uuid4().hex
        self.store.start(name_of(workflow) if callable(workflow) else workflow, list(args), wid, self.clock(), if_exists)
        return wid

    def signal(self, workflow_id, name, payload=None):
        self.store.signal(workflow_id, name, payload, self.clock())

    def cancel(self, workflow_id):
        self.store.cancel(workflow_id, self.clock())

    def describe(self, workflow_id):
        return self.store.describe(workflow_id)

    def history(self, workflow_id):
        return self.store.history(self.describe(workflow_id)["run_id"])

    def result(self, workflow_id, timeout=None, poll=0.02):
        """Waits for the workflow (following continue-as-new) to finish."""
        end = None if timeout is None else time.monotonic() + timeout
        while True:
            d = self.describe(workflow_id)
            if d["status"] == "completed":
                return d["result"]
            if d["status"] in ("failed", "cancelled"):
                raise WorkflowFailed(workflow_id, d["status"], d["result"])
            if end is not None and time.monotonic() > end:
                raise TimeoutError(f"workflow {workflow_id} still {d['status']}")
            time.sleep(poll)

    def query(self, workflow, workflow_id, name, *args):
        """Replays the current run (read only) and asks its query handler."""
        d = self.describe(workflow_id)
        r = Replayer(workflow, workflow_id, d["run_id"], self.store.history(d["run_id"]))
        r.run(self.clock())
        if name not in r.queries:
            raise NotFound(f"workflow {workflow_id} has no query {name}")
        return r.queries[name](*args)
