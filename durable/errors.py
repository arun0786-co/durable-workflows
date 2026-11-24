class WorkflowError(Exception):
    """Base class for errors raised inside workflow code."""


class ActivityError(WorkflowError):
    """An activity failed for good (retries exhausted, or a non-retryable
    error). `type` is the class name of the error the activity raised."""

    def __init__(self, type, message):
        super().__init__(f"{type}: {message}")
        self.type = type
        self.message = message


class ChildError(WorkflowError):
    """A child workflow failed."""

    def __init__(self, type, message):
        super().__init__(f"{type}: {message}")
        self.type = type
        self.message = message


class Cancelled(WorkflowError):
    """Thrown into a workflow when cancellation is requested. The workflow
    may catch it to run compensations."""


class NonDeterminism(Exception):
    """Replaying the history produced different commands than the ones
    recorded: the workflow code changed or depends on something it should
    not (wall time, randomness, a dict of unknown order...)."""


class ContinueAsNew(BaseException):
    """Raised by ctx.continue_as_new(); ends this run and starts a fresh one
    with the given input and an empty history."""

    def __init__(self, args):
        self.args_ = list(args)


def error_info(e):
    return {"type": getattr(e, "type", None) or type(e).__name__, "message": getattr(e, "message", None) or str(e)}
