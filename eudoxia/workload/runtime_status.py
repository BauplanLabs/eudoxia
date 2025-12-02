from enum import Enum
from typing import Dict, List, Optional
from eudoxia.utils.dag import DAGDependencyTracker


class OperatorState(Enum):
    """States an operator can be in during execution"""
    PENDING = "pending"           # Not yet assigned, or suspended and ready for re-assignment
    RUNNING = "running"           # Assigned to a container, currently executing
    SUSPENDING = "suspending"     # Container writing RAM to disk
    COMPLETED = "completed"       # Successfully finished (terminal state)
    FAILED = "failed"             # Execution failed (retry is possible)


# Valid state transitions for each state
VALID_TRANSITIONS: Dict[OperatorState, List[OperatorState]] = {
    OperatorState.PENDING: [
        OperatorState.RUNNING,
        OperatorState.FAILED,
    ],
    OperatorState.RUNNING: [
        OperatorState.SUSPENDING,
        OperatorState.COMPLETED,
        OperatorState.FAILED,
    ],
    OperatorState.SUSPENDING: [
        OperatorState.PENDING,
        OperatorState.COMPLETED,
        OperatorState.FAILED,
    ],
    OperatorState.COMPLETED: [],
    OperatorState.FAILED: [
        OperatorState.RUNNING,
    ],
}


class PipelineRuntimeStatus:
    """
    Mutable execution state for a pipeline.

    Tracks per-operator execution state and dependencies. This is the only mutable
    field in a Pipeline object - all other fields are immutable.
    """

    def __init__(self, pipeline: 'Pipeline'):
        self.pipeline_id = pipeline.pipeline_id
        self.pipeline = pipeline
        self.operator_states: Dict['Operator', OperatorState] = {}
        self.dag_dependency_tracker = DAGDependencyTracker(pipeline.values)
        self.arrival_tick: Optional[int] = None

        for operator in pipeline.values:
            self.operator_states[operator] = OperatorState.PENDING

    def record_arrival(self, tick: int):
        """Record the tick at which this pipeline arrived."""
        self.arrival_tick = tick

    def transition(self, operator: 'Operator', new_state: OperatorState):
        """
        Transition an operator to a new state.

        Raises:
            AssertionError: If the transition is invalid
        """
        current_state = self.operator_states[operator]
        valid_next_states = VALID_TRANSITIONS[current_state]
        assert new_state in valid_next_states, \
            f"Invalid transition: {current_state.value} -> {new_state.value}. " \
            f"Valid: {[s.value for s in valid_next_states]}"

        self.operator_states[operator] = new_state

        if new_state == OperatorState.COMPLETED:
            self.dag_dependency_tracker.mark_success(operator)

    def can_schedule(self, operator: 'Operator') -> tuple[bool, Optional[str]]:
        """
        Check if an operator can be scheduled.

        Returns:
            Tuple of (can_schedule, error_reason)
        """
        state = self.operator_states[operator]

        if state == OperatorState.RUNNING:
            return (False, "Operator already running")

        if state == OperatorState.SUSPENDING:
            return (False, "Operator suspending")

        if state == OperatorState.COMPLETED:
            return (False, "Operator already completed")

        # FAILED operators can be scheduled (for retry)
        if not self.dag_dependency_tracker.all_dependencies_satisfied(operator):
            return (False, "Dependencies not satisfied")

        return (True, None)

    def is_pipeline_complete(self) -> bool:
        """Check if all operators are in COMPLETED or FAILED state."""
        for state in self.operator_states.values():
            if state not in (OperatorState.COMPLETED, OperatorState.FAILED):
                return False
        return True

    def is_pipeline_successful(self) -> bool:
        """Check if all operators completed successfully."""
        return all(state == OperatorState.COMPLETED for state in self.operator_states.values())
