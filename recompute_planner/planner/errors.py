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
