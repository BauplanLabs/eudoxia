from .pipeline import Pipeline, Operator, Segment
import uuid
from eudoxia.utils import Priority
from typing import List

class Suspend:
    """
    Object tracks suspended jobs
    """
    def __init__(self, cid: uuid.UUID, pool_id: int):
        self.cid = cid
        self.pool_id = pool_id

class Failure: 
    """
    Tracks when jobs fail and tracks error message to provide back to scheduler
    """
    def __init__(self, ops: List[Operator], cpu, ram, priority: Priority,
                 pool_id: int, cid = None, error: str = None): 
        self.ops = ops
        self.cpu = cpu
        self.ram = ram
        self.priority = priority
        self.pool_id = pool_id
        self.cid = cid
        self.error = error

    def __repr__(self):
        return f"Failed to run container {self.cid} with error: {self.error}"

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
                 pool_id: int, cid = None, is_resume=False, force_run = False): 
        self.ops = ops
        self.cpu = cpu
        self.ram = ram
        self.priority = priority
        self.pool_id = pool_id
        self.cid = cid
        self.is_resume = is_resume
        self.force_run = force_run

    def __repr__(self):
        return f"Assignment: allocate container with {self.cpu} CPUs, {self.ram}GB RAM"
