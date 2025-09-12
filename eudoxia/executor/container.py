import logging
import uuid
import numpy as np
from typing import List
from eudoxia.utils import DISK_SCAN_GB_SEC, Priority
from eudoxia.workload import Operator

logger = logging.getLogger(__name__)


class Container: 
    """
    An encapsulation of CPU, RAM, and a list of operators. A container is
    created and then calculates how many ticks it will need to run with
    resources provided. 
    """
    def __init__(self, ram, cpu, ops, prty: Priority, pool_id: int, rng: np.random.Generator, ticks_per_second: int):
        self.cid = uuid.UUID(bytes=rng.bytes(16))
        self.pool_id = pool_id
        self.ram = ram
        self.cpu = cpu
        self.operators: List[Operator] = ops
        self.segment_tick_boundaries = []
        self.suspend_ticks = None
        self._suspend_ticks_left = None
        self.priority = prty
        self.error: str = None
        self.ticks_per_second = ticks_per_second
        self.tick_length_secs = 1.0 / ticks_per_second

        self.num_ticks = self._compute_ticks() 
        self._num_ticks_left = self.num_ticks
        self.num_secs = self.num_ticks * self.tick_length_secs
        logger.info(self.__repr__())
    
    def __repr__(self):
        return (f"Container {self.cid} with {self.cpu} CPUs, {self.ram}GB RAM, " + \
                f"will run for {self.num_ticks} ticks or {self.num_secs:.2f} seconds")

    def _compute_ticks(self) -> int:
        """
        This function utilizes functions provided by Segment to calculate the
        amount of CPU and RAM ticks that are needed across all segments
        Returns:
            int: number of ticks this container will need to run for
        """
        total_ticks = 0
        for op in self.operators:
            for seg in op.get_segments():
                # will it OOM, and if so, when?
                oom_seconds = seg.get_seconds_until_oom(self.ram)

                if oom_seconds is not None:
                    # compute how long it it will be until the OOM occurs
                    self.error = "OOM"
                    seg_ticks_before_OOM = int(oom_seconds / self.tick_length_secs)
                    total_ticks += seg_ticks_before_OOM
                else:
                    # there is no OOM.  We will spend all the time
                    # expected on I/O (first), then CPU (second)
                    io_time_secs = seg.get_io_seconds()
                    cpu_time_secs = seg.get_cpu_time(self.cpu)
                    total_ticks += int((io_time_secs + cpu_time_secs) / self.tick_length_secs)
                self.segment_tick_boundaries.append(total_ticks)
        return total_ticks

    def tick(self):
        self._num_ticks_left -= 1

    def is_completed(self):
        return (self._num_ticks_left == 0)

    def can_suspend_container(self) -> bool:
        """
        Can only suspend a container if we're in the middle of executing a
        Segment. Must wait for Segment to complete before the container is
        pausable
        """
        elapsed = self.num_ticks - self._num_ticks_left
        if elapsed in self.segment_tick_boundaries:
            return True
        return False

    def suspend_container(self):
        """
        Suspend container execution, free CPUs and RAM. Requires writing
        current data to disk.  
        """
        write_to_disk_ticks = self.ram / DISK_SCAN_GB_SEC
        self.suspend_ticks = write_to_disk_ticks
        self._suspend_ticks_left = write_to_disk_ticks

    def suspend_container_tick(self):
        self._suspend_ticks_left -= 1

    def is_suspended(self) -> bool:
        return (self._suspend_ticks_left == 0)
