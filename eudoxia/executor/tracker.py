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
        # Validate all operators in the assignment
        for op in assignment.ops:
            tracker = self.get_or_create_tracker(op.pipeline)
            if not tracker.all_dependencies_satisfied(op, assignment.ops):
                return "dependency"
        return None

    def mark_operators_success(self, ops: List):
        """Mark a list of operators as successfully completed and clean up completed pipelines."""
        for op in ops:
            pipeline_id = op.pipeline.pipeline_id
            tracker = self.dependency_trackers[pipeline_id]
            tracker.mark_success(op)

            # Clean up tracker if all nodes in pipeline are complete
            if tracker.all_nodes_complete():
                del self.dependency_trackers[pipeline_id]
