from enum import Enum
from typing import Dict, List, Optional, Union


class OperatorState(Enum):
    """States an operator can be in during execution"""
    PENDING = "pending"           # Not yet assigned, available for scheduling
    ASSIGNED = "assigned"         # Assignment object created containing this ops (container assignment is imminent)
    RUNNING = "running"           # Currently executing in a container
    SUSPENDING = "suspending"     # Container writing RAM to disk
    COMPLETED = "completed"       # Successfully finished (terminal state)
    FAILED = "failed"             # Execution failed (retry is possible)


class OperatorStatus:
    """Tracks both the execution state and pool assignment for an operator."""

    def __init__(self):
        self.state: OperatorState = OperatorState.PENDING
        self.pool_id: Optional[int] = None


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

# States from which an operator can be assigned (can transition to ASSIGNED)
ASSIGNABLE_STATES = frozenset(
    state for state, transitions in VALID_TRANSITIONS.items()
    if OperatorState.ASSIGNED in transitions
)


class PipelineRuntimeStatus:
    """
    Mutable execution state for a pipeline.

    Tracks per-operator execution state and dependencies. This is the only mutable
    field in a Pipeline object - all other fields are immutable.
    """

    def __init__(self, pipeline: 'Pipeline'):
        self.pipeline = pipeline
        self.operator_status: Dict['Operator', OperatorStatus] = {}
        self.state_counts: Dict[OperatorState, int] = {state: 0 for state in OperatorState}
        self.arrival_tick: Optional[int] = None
        self.finish_tick: Optional[int] = None

        for operator in pipeline.values:
            self.operator_status[operator] = OperatorStatus()
            self.state_counts[OperatorState.PENDING] += 1

    def record_arrival(self, tick: int):
        """Record the tick at which this pipeline arrived."""
        assert self.arrival_tick is None, "arrival_tick already recorded"
        self.arrival_tick = tick

    def check_transition(self, operator: 'Operator', new_state: OperatorState, pool_id: Optional[int] = None, enforce_locality: bool = False) -> tuple[bool, Optional[str]]:
        """
        Check if a state transition is valid.

        Returns:
            Tuple of (can_transition, error_reason)
        """
        current_state = self.operator_status[operator].state

        if new_state not in VALID_TRANSITIONS[current_state]:
            return (False, f"Cannot transition operator {operator.id} in pipeline {operator.pipeline.pipeline_id} from {current_state.value} to {new_state.value}")

        # Check suspend/resume locality: if this op was previously run on a specific
        # pool, it must be assigned back to the same pool
        if enforce_locality and new_state == OperatorState.ASSIGNED and pool_id is not None:
            existing_pool = self.operator_status[operator].pool_id
            if existing_pool is not None and existing_pool != pool_id:
                return (False, f"Locality violation: operator was previously run on pool {existing_pool}, assigned to pool {pool_id}")

        # Dependencies checked when starting execution, not when assigning
        if new_state == OperatorState.RUNNING:
            for parent in operator.parents:
                if self.operator_status[parent].state != OperatorState.COMPLETED:
                    return (False, "Dependencies not satisfied")

        return (True, None)

    def transition(self, operator: 'Operator', new_state: OperatorState, pool_id: Optional[int] = None, enforce_locality: bool = False):
        """
        Transition an operator to a new state.

        Raises:
            AssertionError: If the transition is invalid
        """
        can_transition, error = self.check_transition(operator, new_state, pool_id=pool_id, enforce_locality=enforce_locality)
        assert can_transition, error
        old_state = self.operator_status[operator].state
        self.state_counts[old_state] -= 1
        self.state_counts[new_state] += 1
        self.operator_status[operator].state = new_state
        if pool_id is not None:
            self.operator_status[operator].pool_id = pool_id

    def is_pipeline_successful(self) -> bool:
        """Check if all operators completed successfully."""
        return self.state_counts[OperatorState.COMPLETED] == len(self.operator_status)

    def record_finish(self, tick: int):
        """Record the tick at which this pipeline finished."""
        assert self.finish_tick is None, "finish_tick already recorded"
        self.finish_tick = tick

    def get_latency_ticks(self) -> int:
        """Get the latency in ticks (finish_tick - arrival_tick)."""
        assert self.arrival_tick is not None, "arrival_tick not recorded"
        assert self.finish_tick is not None, "finish_tick not recorded"
        return self.finish_tick - self.arrival_tick

    def get_ops(self, state: Union[OperatorState, List[OperatorState]], require_parents_complete: bool = False) -> List['Operator']:
        """
        Get operators matching the given filter criteria.

        Args:
            state: Only include operators in this state (or one of these states if a list)
            require_parents_complete: If True, only include operators whose parents are all COMPLETED

        Returns:
            List of operators matching the criteria
        """
        # Normalize to a list for consistent checking
        if isinstance(state, OperatorState):
            allowed_states = [state]
        else:
            allowed_states = state

        result = []
        for op, op_state in ((op, s.state) for op, s in self.operator_status.items()):
            if op_state not in allowed_states:
                continue
            if require_parents_complete and not all(
                self.operator_status[p].state == OperatorState.COMPLETED for p in op.parents
            ):
                continue
            result.append(op)
        return result
