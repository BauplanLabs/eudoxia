from dataclasses import dataclass
from typing import List, Optional
import uuid
from eudoxia.workload import Pipeline, Operator
from eudoxia.utils import Priority


@dataclass
class RetryStats:
    """Stats from a previous failed/suspended container, used to inform retry allocation."""
    old_ram: int
    old_cpu: int
    error: str
    container_id: uuid.UUID
    pool_id: int


class WaitingQueueJob:
    def __init__(self, priority: Priority, p: Pipeline = None,
                 ops: List[Operator] = None,
                 retry_stats: Optional[RetryStats] = None):
        self.priority = priority
        self.pipeline = p
        self.ops = ops
        self.retry_stats = retry_stats  # present if this job is a retry