from typing import List
import uuid
from eudoxia.workload import Pipeline, Operator
from eudoxia.utils import Priority


class WaitingQueueJob: 
    def __init__(self, priority: Priority, p: Pipeline=None, ops:
                 List[Operator]=None, pool_id: int = None,
                 container_id: str = None, old_ram: int = None, old_cpu: int =
                 None, error: str = None): 
        self.priority = priority
        self.pipeline = p
        self.ops = ops
        self.pool_id = pool_id
        self.container_id = container_id
        self.old_ram = old_ram
        self.old_cpu = old_cpu
        self.error = error