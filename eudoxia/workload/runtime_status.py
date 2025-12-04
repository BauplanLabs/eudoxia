from enum import Enum
from typing import Dict, List, Optional


class OperatorState(Enum):
    """States an operator can be in during execution"""
    PENDING = "pending"           # Not yet assigned, available for scheduling
    ASSIGNED = "assigned"         # In a container, waiting for turn to execute
    RUNNING = "running"           # Currently executing in a container
    SUSPENDING = "suspending"     # Container writing RAM to disk
    COMPLETED = "completed"       # Successfully finished (terminal state)
    FAILED = "failed"             # Execution failed (retry is possible)


# Valid state transitions for each state
VALID_TRANSITIONS: Dict[OperatorState, List[OperatorState]] = {
    OperatorState.PENDING: [
        OperatorState.ASSIGNED,
    ],
    OperatorState.ASSIGNED: [
        OperatorState.RUNNING,
        OperatorState.SUSPENDING,
        OperatorState.FAILED,
    ],
    OperatorState.RUNNING: [
        OperatorState.COMPLETED,
        OperatorState.FAILED,
    ],
    OperatorState.SUSPENDING: [
        OperatorState.PENDING,
    ],
    OperatorState.COMPLETED: [],
    OperatorState.FAILED: [
        OperatorState.ASSIGNED,
    ],
}


class PipelineRuntimeStatus:
    """
    Mutable execution state for a pipeline.

    Tracks per-operator execution state and dependencies. This is the only mutable
    field in a Pipeline object - all other fields are immutable.
    """

    def __init__(self, pipeline: 'Pipeline'):
        self.pipeline = pipeline
        self.operator_states: Dict['Operator', OperatorState] = {}
        self.arrival_tick: Optional[int] = None

        for operator in pipeline.values:
            self.operator_states[operator] = OperatorState.PENDING

    def record_arrival(self, tick: int):
        """Record the tick at which this pipeline arrived."""
        self.arrival_tick = tick

    def check_transition(self, operator: 'Operator', new_state: OperatorState) -> tuple[bool, Optional[str]]:
        """
        Check if a state transition is valid.

        Returns:
            Tuple of (can_transition, error_reason)
        """
        current_state = self.operator_states[operator]

        if new_state not in VALID_TRANSITIONS[current_state]:
            return (False, f"Cannot transition from {current_state.value} to {new_state.value}")

        # Dependencies checked when starting execution, not when assigning
        if new_state == OperatorState.RUNNING:
            for parent in operator.parents:
                if self.operator_states[parent] != OperatorState.COMPLETED:
                    return (False, "Dependencies not satisfied")

        return (True, None)

    def transition(self, operator: 'Operator', new_state: OperatorState):
        """
        Transition an operator to a new state.

        Raises:
            AssertionError: If the transition is invalid
        """
        can_transition, error = self.check_transition(operator, new_state)
        assert can_transition, error
        self.operator_states[operator] = new_state

    def is_pipeline_successful(self) -> bool:
        """Check if all operators completed successfully."""
        return all(state == OperatorState.COMPLETED for state in self.operator_states.values())
