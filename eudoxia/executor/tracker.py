from typing import Dict, List, Optional
from eudoxia.utils.dag import DAGDependencyTracker
from .assignment import Assignment


class DependencyTracker:
    """
    Manages dependency tracking for all pipelines during execution.
    Shared across all ResourcePools to maintain consistent execution state.
    """
    def __init__(self):
        self.dependency_trackers: Dict[str, DAGDependencyTracker] = {}

    def get_or_create_tracker(self, pipeline) -> DAGDependencyTracker:
        """Get existing tracker or create a new one for this pipeline."""
        if pipeline.pipeline_id not in self.dependency_trackers:
            self.dependency_trackers[pipeline.pipeline_id] = DAGDependencyTracker(pipeline.values)
        return self.dependency_trackers[pipeline.pipeline_id]

    def verify_assignment_dependencies(self, assignment: Assignment) -> Optional[str]:
        """
        Verify that operator dependencies are satisfied before assignment.
        For each operator, all parent operators must either be:
        - Already complete (marked succeeded in the pipeline's dependency tracker), OR
        - Part of the current assignment (being assigned together)

        Returns:
            None if dependencies are satisfied, error message string if not
        """
        # Get or create tracker for this pipeline
        first_op = assignment.ops[0]  # At least one operator exists
        tracker = self.get_or_create_tracker(first_op.pipeline)

        # Validate all operators in the assignment
        for op in assignment.ops:
            if not tracker.all_parents_ready(op, assignment.ops):
                return f"Invalid assignment: operator {op.id} has incomplete parent dependencies"
        return None

    def mark_operators_success(self, ops: List):
        """Mark a list of operators as successfully completed."""
        for op in ops:
            tracker = self.dependency_trackers[op.pipeline.pipeline_id]
            tracker.mark_success(op)
