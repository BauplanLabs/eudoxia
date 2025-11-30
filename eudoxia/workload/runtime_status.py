from enum import Enum
from typing import Dict, List, Optional, Set
from eudoxia.utils.dag import DAGDependencyTracker


class OperatorState(Enum):
    """States an operator can be in during execution"""
    PENDING = "pending"           # Not yet assigned, or suspended and ready for re-assignment
    RUNNING = "running"           # Assigned to a container, currently executing
    SUSPENDING = "suspending"     # Container writing RAM to disk
    COMPLETED = "completed"       # Successfully finished (terminal state)
    FAILED = "failed"            # Execution failed


# Valid state transitions for each state
VALID_TRANSITIONS: Dict[OperatorState, List[OperatorState]] = {
    OperatorState.PENDING: [
        OperatorState.RUNNING,    # Assigned to container
        OperatorState.FAILED,     # Failed validation before running
    ],
    OperatorState.RUNNING: [
        OperatorState.SUSPENDING, # Container being suspended
        OperatorState.COMPLETED,  # Execution succeeded
        OperatorState.FAILED,     # Execution failed (e.g., OOM)
    ],
    OperatorState.SUSPENDING: [
        OperatorState.PENDING,    # Suspension complete, ready for re-assignment
        OperatorState.COMPLETED,  # Completed during suspension
        OperatorState.FAILED,     # Failed during suspension
    ],
    OperatorState.COMPLETED: [],  # Terminal state - no transitions allowed
    OperatorState.FAILED: [
        OperatorState.RUNNING,    # Retry by re-assigning
    ],
}


class PipelineRuntimeStatus:
    """
    Mutable execution state for a pipeline.

    Tracks per-operator execution state and dependencies. This is the only mutable
    field in a Pipeline object - all other fields are immutable.
    """

    def __init__(self, pipeline: 'Pipeline'):
        """
        Initialize runtime status for a pipeline.

        Args:
            pipeline: The pipeline this status tracks
        """
        self.pipeline_id = pipeline.pipeline_id
        self.pipeline = pipeline  # Back-reference for convenience

        # Per-operator state tracking
        self.operator_states: Dict['Operator', OperatorState] = {}
        self.operator_containers: Dict['Operator', str] = {}  # operator -> container_id (for running/suspending ops)
        self.operator_errors: Dict['Operator', str] = {}  # operator -> error message (for failed ops)

        # Initialize all operators as PENDING
        for operator in pipeline.values:
            self.operator_states[operator] = OperatorState.PENDING

        # Dependency tracking (reuses existing DAGDependencyTracker)
        self.dag_dependency_tracker = DAGDependencyTracker(pipeline.values)

        # Timing information
        self.arrival_tick: Optional[int] = None
        self.first_assignment_tick: Optional[int] = None
        self.completion_tick: Optional[int] = None

    def record_arrival(self, tick: int):
        """
        Record the tick at which this pipeline arrived.

        Args:
            tick: The tick number when the pipeline entered the system
        """
        self.arrival_tick = tick

    def transition(self, operator: 'Operator', new_state: OperatorState, **kwargs):
        """
        Transition an operator to a new state.

        Args:
            operator: The operator to transition
            new_state: The new state to transition to
            **kwargs: Additional arguments depending on the transition:
                - container_id (str): Required when transitioning to RUNNING
                - error (str): Optional when transitioning to FAILED

        Raises:
            AssertionError: If the transition is invalid
        """
        current_state = self.operator_states[operator]

        # Validate transition
        valid_next_states = VALID_TRANSITIONS[current_state]
        assert new_state in valid_next_states, \
            f"Invalid transition for operator: {current_state.value} -> {new_state.value}. " \
            f"Valid transitions from {current_state.value}: {[s.value for s in valid_next_states]}"

        # Update state
        self.operator_states[operator] = new_state

        # Handle side effects based on new state
        if new_state == OperatorState.RUNNING:
            # Must provide container_id when transitioning to RUNNING
            assert 'container_id' in kwargs, "container_id required when transitioning to RUNNING state"
            self.operator_containers[operator] = kwargs['container_id']

        elif new_state == OperatorState.COMPLETED:
            # Mark operator as successful in dependency tracker
            self.dag_dependency_tracker.mark_success(operator)
            # Clean up container mapping
            if operator in self.operator_containers:
                del self.operator_containers[operator]

        elif new_state == OperatorState.FAILED:
            # Store error message if provided
            if 'error' in kwargs:
                self.operator_errors[operator] = kwargs['error']
            # Clean up container mapping
            if operator in self.operator_containers:
                del self.operator_containers[operator]

        elif new_state == OperatorState.PENDING:
            # Clean up container mapping (for suspended -> pending or failed -> pending transitions)
            if operator in self.operator_containers:
                del self.operator_containers[operator]
            # Clean up error if retrying from failed
            if operator in self.operator_errors:
                del self.operator_errors[operator]

    # === Query methods for Scheduler ===

    def get_ready_operators(self) -> List['Operator']:
        """
        Get operators that are ready to be scheduled.

        An operator is ready if:
        1. It is in PENDING state (not already running/complete/failed)
        2. All its dependencies are satisfied (parents are completed)

        Returns:
            List of operators ready for assignment
        """
        ready = []
        for op in self.pipeline.values:
            if (self.operator_states[op] == OperatorState.PENDING and
                self.dag_dependency_tracker.all_dependencies_satisfied(op)):
                ready.append(op)
        return ready

    def can_schedule(self, operator: 'Operator') -> tuple[bool, Optional[str]]:
        """
        Check if an operator can be scheduled.

        Args:
            operator: The operator to check

        Returns:
            Tuple of (can_schedule, error_reason)
            - can_schedule: True if operator can be scheduled, False otherwise
            - error_reason: String describing why operator cannot be scheduled, or None if it can
        """
        state = self.operator_states[operator]

        if state == OperatorState.RUNNING:
            container_id = self.operator_containers.get(operator, "unknown")
            return (False, f"Operator already running in container {container_id}")

        if state == OperatorState.SUSPENDING:
            container_id = self.operator_containers.get(operator, "unknown")
            return (False, f"Operator suspending in container {container_id}")

        if state == OperatorState.COMPLETED:
            return (False, "Operator already completed")

        # FAILED operators can be scheduled (for retry) - this is backwards compatible
        # with schedulers that re-queue failed jobs

        # Check dependencies (for PENDING and FAILED operators)
        if not self.dag_dependency_tracker.all_dependencies_satisfied(operator):
            return (False, "Dependencies not satisfied")

        return (True, None)

    def is_pipeline_complete(self) -> bool:
        """
        Check if the entire pipeline has completed execution.

        A pipeline is complete when all operators are in COMPLETED or FAILED state.

        Returns:
            True if pipeline is complete, False otherwise
        """
        for state in self.operator_states.values():
            if state not in (OperatorState.COMPLETED, OperatorState.FAILED):
                return False
        return True

    def is_pipeline_successful(self) -> bool:
        """
        Check if the pipeline completed successfully (all operators COMPLETED).

        Returns:
            True if all operators completed successfully, False otherwise
        """
        return all(state == OperatorState.COMPLETED for state in self.operator_states.values())

    def get_state_summary(self) -> Dict[OperatorState, int]:
        """
        Get count of operators in each state.

        Useful for debugging and statistics.

        Returns:
            Dictionary mapping state to count of operators in that state
        """
        summary = {state: 0 for state in OperatorState}
        for state in self.operator_states.values():
            summary[state] += 1
        return summary

    def get_operators_in_state(self, state: OperatorState) -> List['Operator']:
        """
        Get all operators currently in a specific state.

        Args:
            state: The state to filter by

        Returns:
            List of operators in the specified state
        """
        return [op for op, op_state in self.operator_states.items() if op_state == state]

    def get_running_containers(self) -> Set[str]:
        """
        Get set of all container IDs currently running operators from this pipeline.

        Returns:
            Set of container IDs
        """
        return set(self.operator_containers.values())
