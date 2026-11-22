"""Error types for the recompute planner."""


class PlannerError(Exception):
    """Base error for the recompute planner."""


class GraphError(PlannerError):
    """Invalid dependency graph: missing edge, cycle, contradictory ranges."""


class StateError(PlannerError):
    """Invalid or inconsistent persisted state (e.g. missing evidence)."""


class PlanError(PlannerError):
    """Plan lifecycle violation."""


class EventError(PlannerError):
    """Invalid late-arriving event."""


class SnapshotError(PlannerError):
    """Invalid upstream row snapshot: bad rows, duplicate ids, version/content
    mismatch, or a node that is not an attribution upstream."""


class DeliveryError(PlannerError):
    """A consistent cross-day delivery could not be produced or written.

    ``code`` distinguishes the failure for callers and tests:
    ``invalid_range`` / ``invalid_target`` / ``unknown_node`` /
    ``not_attribution`` / ``no_candidate`` (some day has no usable version,
    or one of its recorded inputs has no registered snapshot) /
    ``no_solution`` (versions exist on every day but cannot be combined) /
    ``state`` (inconsistent stored run) / ``read`` / ``write``.
    """

    def __init__(self, message, code="delivery"):
        super().__init__(message)
        self.code = code
