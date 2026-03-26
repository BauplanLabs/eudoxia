# Idea
# Minimize the inital queueing delay by letting small tasks run first
# Sort the ready queue by estimated CPU time

from typing import List, Dict, Tuple
from eudoxia.scheduler.base import Scheduler
from eudoxia.scheduler.decorators import register_scheduler
from eudoxia.executor.assignment import Assignment
from eudoxia.workload.pipeline import Operator

@register_scheduler("sjf")
class ShortestJobFirstScheduler(Scheduler):
    """
    SJF Scheduler: Prioritizes operators with the smallest estimated CPU time.
    This demonstrates how an AI estimator's accuracy impacts queueing delay.
    """

    def run_one_tick(self, executor_results, new_pipelines) -> Tuple[List[Assignment], List[Assignment]]:
        # 1. Update internal state based on executor results
        self.executor.update_status(executor_results)

        # 2. Add new pipelines to the tracking system
        for pipeline in new_pipelines:
            self.workload.add_pipeline(pipeline)

        # 3. Get all operators that are ready to be assigned
        ready_ops = self.get_ready_operators()

        # --- THE CORE SJF LOGIC ---
        # We sort the ready operators by their estimated CPU time.
        # If 'cpu_time_est' is missing, we treat it as a very long job (float('inf')).
        ready_ops.sort(key=lambda op: op.estimate.get("cpu_time_est", float('inf')))
        # --------------------------

        assignments = []
        # 4. Try to assign sorted operators to available resources
        for op in ready_ops:
            # Note: try_assign handles the memory/CPU capacity checks
            assignment = self.executor.try_assign(op)
            if assignment:
                assignments.append(assignment)
        
        # SJF typically doesn't use suspensions (preemption), so return empty list for that.
        return [], assignments

    def get_ready_operators(self) -> List[Operator]:
        """Helper to find all operators currently in an assignable state."""
        ready = []
        for pipeline in self.workload.pipelines.values():
            for op in pipeline.values:
                # Check if the operator's parents are done and it's not already running
                if pipeline.is_operator_ready(op):
                    ready.append(op)
        return ready