from eudoxia.workload.pipeline import Pipeline, Operator, Segment
from eudoxia.workload import OperatorState
import uuid
from eudoxia.utils import Priority
from typing import List

class Suspend:
    """
    Object tracks suspended jobs
    """
    def __init__(self, container_id: str, pool_id: int):
        self.container_id = container_id
        self.pool_id = pool_id

class ExecutionResult:
    """
    Tracks execution results for containers, both successes and failures.
    For failures, the error field contains an error message.
    """
    def __init__(self, ops: List[Operator], cpu, ram, priority: Priority,
                 pool_id: int, container_id = None, error: str = None):
        self.ops = ops
        self.cpu = cpu
        self.ram = ram
        self.priority = priority
        self.pool_id = pool_id
        self.container_id = container_id
        self.error = error

    def failed(self) -> bool:
        """Returns True if this execution result represents a failure."""
        return self.error is not None

    def to_dict(self) -> dict:
        """Serialize execution result to JSON-compatible dict."""
        return {
            "ops": [str(op.id) for op in self.ops],
            "cpu": self.cpu,
            "ram": self.ram,
            "priority": self.priority.name,
            "pool_id": self.pool_id,
            "container_id": self.container_id,
            "error": self.error,
        }

    def __repr__(self):
        if self.error is not None:
            return f"container failed {self.error}: container={self.container_id} cpus={self.cpu} ram_gb={self.ram}"
        else:
            return f"container succeeded: container={self.container_id} cpus={self.cpu} ram_gb={self.ram}"

class Assignment:
    """
    Class defines an object returned by the scheduler to the execution engine.
    Each object encapsulates a list of operators to execute and the number of
    CPUs and RAM to allocate to a container. The Execution Engine then allocates
    a container to run the task list.

    Args:
        ops (list[operator]): list of operators
        cpu (int): number of CPUs to allocate
        ram (int): GB of RAM to allocate
        force_run (bool): whether to force the EE to allocate more resources
                          rather than queue the assignment if there are insufficient 
                          resources
    """
    def __init__(self, ops: List[Operator], cpu, ram, priority: Priority,
                 pool_id: int, pipeline_id: str, container_id = None, is_resume=False, force_run = False):
        assert len(ops) > 0, f"assignments cannot have zero operators"
        assert cpu > 0, f"must assign positive CPU allocation, got {cpu}"
        assert ram > 0, f"must assign positive RAM allocation, got {ram}"
        # Transition operators to ASSIGNED immediately when the Assignment is created,
        # rather than waiting for the Container to be created. This prevents schedulers
        # from creating duplicate assignments for the same operator within a single tick
        # (e.g., assigning the same op to multiple pools before the executor runs).
        for op in ops:
            op.transition(OperatorState.ASSIGNED)
        self.ops = ops
        self.cpu = cpu
        self.ram = ram
        self.priority = priority
        self.pool_id = pool_id
        self.pipeline_id = pipeline_id
        self.container_id = container_id
        # TODO: Resume cost (read from disk) is not yet modeled.
        # Currently suspension writes to disk but resume is instant.
        self.is_resume = is_resume
        self.force_run = force_run

    def __repr__(self):
        return f"Assignment: allocate container with {self.cpu} CPUs, {self.ram}GB RAM for pipeline {self.pipeline_id}"
