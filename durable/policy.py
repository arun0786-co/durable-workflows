from dataclasses import asdict, dataclass, field


@dataclass
class RetryPolicy:
    """How an activity is retried. Attempts that raise, or whose worker dies
    (the lease runs out), count as failures."""

    max_attempts: int = 5
    initial: float = 1.0  # seconds before the second attempt
    backoff: float = 2.0
    max_interval: float = 60.0
    non_retryable: list = field(default_factory=list)  # error class names that fail at once

    def delay(self, attempt):
        """Wait after failed attempt number `attempt` (1-based)."""
        return min(self.initial * self.backoff ** (attempt - 1), self.max_interval)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d) if d else cls()
