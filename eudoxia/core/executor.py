import logging
import uuid
import  numpy as np
from typing import List, Dict, Tuple
from eudoxia.utils import EudoxiaException, DISK_SCAN_GB_SEC, TICK_LENGTH_SECS, Priority
from eudoxia.utils import Suspend, Assignment, Failure
from eudoxia.utils import Segment, Operator, Pipeline
logger = logging.getLogger(__name__)

class Container: 
    """
    An encapsulation of CPU, RAM, and a list of operators. A container is
    created and then calculates how many ticks it will need to run with
    resources provided. 
    """
    def __init__(self, ram, cpu, ops, prty: Priority, pool_id: int, rng: np.random.Generator):
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

        self.num_ticks = self._compute_ticks() 
        self._num_ticks_left = self.num_ticks
        self.num_secs = self.num_ticks * TICK_LENGTH_SECS
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
            for seg in op.values:
                if seg.io > self.ram:
                    # set that the container throws an error when it hits this
                    # many ticks
                    self.error = "OOM"

                    # it will run for as many ticks as it took to get here, plus
                    # however long it would take to read the data needed for
                    # this container's RAM total, as it will OOM when it gets to
                    # that point, not when it reads the entire segment's RAM
                    seg_ticks_before_OOM = int((self.ram / DISK_SCAN_GB_SEC) / TICK_LENGTH_SECS)
                    total_ticks += seg_ticks_before_OOM

                scan_time_ticks = seg.get_io_ticks()
                cpu_time_ticks = seg.scale_cpu_time_ticks(self.cpu)

                total_ticks += scan_time_ticks
                total_ticks += cpu_time_ticks
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

class ResourcePool:
    """
    Manager of a pool of resources and active containers, the Executor takes
    assignments and ensures that all costs and resources are accounted for and
    additional are allocated if instructed.
    """
    def __init__(self, pool_id, cpu_pool, ram_pool, rng, **kwargs):
        self.pool_id = pool_id
        self.max_cpu_pool = cpu_pool
        self.max_ram_pool = ram_pool
        self.avail_cpu_pool = cpu_pool
        self.avail_ram_pool = ram_pool
        self.rng = rng

        # List of actively running and suspended containers
        self.active_containers: List[Container] = [] 
        self.suspending_containers: List[Container] = [] 
        self.suspended_containers: List[Container] = [] 


        # STATS
        self.num_completed = 0
        self.container_tick_times = [] 
        self.cost = 0 # add force_run logic here 
        self.i = 0
        self.outfile_name = f"pool_{self.pool_id}_utility.csv"
        self.outfile = open(self.outfile_name, 'w')

    def verify_valid_assignment(self, assignments: List[Assignment]):
        """
        ensure no overallocation is occuring
        """
        cpu_to_be_alloc = 0
        ram_to_be_alloc = 0
        for a in assignments:
            cpu_to_be_alloc += a.cpu
            ram_to_be_alloc += a.ram

        assert cpu_to_be_alloc <= self.avail_cpu_pool, "Overallocated CPU in assignment"
        assert ram_to_be_alloc <= self.avail_ram_pool, "Overallocated RAM in assignment"

    def get_container_by_id(self, cid: uuid.UUID) -> Container: 
        for container in self.active_containers:
            if container.cid == cid:
                return container
        return None

    def verify_valid_suspend(self, suspensions: List[Suspend]): 
        """
        ensure suspension is valid
        """
        for s in suspensions:
            container = self.get_container_by_id(s.cid)
            assert container.can_suspend_container(), "Container cannot be suspended right now"

    def status_report(self):
        logger.info(f"----------STATUS REPORT POOL {self.pool_id}----------")
        for c in self.active_containers:
            secs_left = c._num_ticks_left * TICK_LENGTH_SECS 
            logger.info(f"{c.cid} running with {c._num_ticks_left} ticks left or {secs_left} seconds left")
        logger.info(f"----------END STATUS REPORT FOR POOL {self.pool_id}----------")

    def log_pool_utilization(self): 
        cpu_util = 100.0 * self.avail_cpu_pool / self.max_cpu_pool
        ram_util = 100.0 * self.avail_ram_pool / self.max_ram_pool
        out_line = f"{self.i}, {cpu_util:.2f}, {ram_util:.2f}\n"
        self.outfile.write(out_line)

    def run_one_tick(self, suspensions: List[Suspend], 
                     assignments: List[Assignment]) -> List[Failure]:
        """
        Run a single tick for the executor, decrement remaining ticks for all
        active containers, remove completed ones, and update relevant
        statistics.
        """
        self.i += 1
        if len(suspensions) > 0:
            self.verify_valid_suspend(suspensions)
            for s in suspensions:
                container = self.get_container_by_id(s.cid)
                container.suspend_container()
                self.suspending_containers.append(container)
                self.active_containers.remove(container)
        
        if len(assignments) > 0:
            self.verify_valid_assignment(assignments)
            for a in assignments:
                container = Container(ram=a.ram, cpu=a.cpu, ops=a.ops,
                                      prty=a.priority, pool_id=self.pool_id, rng=self.rng)
                self.avail_cpu_pool -= a.cpu
                self.avail_ram_pool -= a.ram
                self.active_containers.append(container)
                self.container_tick_times.append(container.num_ticks)

        to_remove = []
        for c in self.suspending_containers:
            c.suspend_container_tick()
            if c.is_suspended():
                self.avail_cpu_pool += c.cpu
                self.avail_ram_pool += c.ram
                to_remove.append(c)
        for c in to_remove:
            self.suspending_containers.remove(c)
            self.suspended_containers.append(c)

        to_remove = []
        failures = []
        for c in self.active_containers:
            c.tick()
            if c.is_completed(): 
                # self.status_report()
                self.avail_cpu_pool += c.cpu
                self.avail_ram_pool += c.ram 
                to_remove.append(c)

                # record a job failure and log it to the console
                if c.error is not None:
                    f = Failure(ops=c.operators, cpu=c.cpu, ram=c.ram,
                                priority=c.priority, pool_id=c.pool_id, cid=c.cid, error=c.error)
                    logger.info(f)
                    failures.append(f)
                else:
                    logger.info(f"Completed container {c.cid}; Freeing {c.cpu} CPUs and {c.ram}GB RAM")
                    self.num_completed += 1
        for c in to_remove:
            self.active_containers.remove(c)

        self.log_pool_utilization()
        return failures


class Executor:
    """
    Manager of a pool of resources and active containers, the Executor takes
    assignments and ensures that all costs and resources are accounted for and
    additional are allocated if instructed.
    """
    def __init__(self, num_pools, cpu_pool, ram_pool, rng, **kwargs):
        # total amount of resources allocated
        self.num_pools = num_pools
        self.max_cpus = cpu_pool
        self.max_ram = ram_pool
        self.rng = rng
        
        # computation for dividing ram, cpu among diff pools
        cpu_per_pool, cpu_remainder = divmod(self.max_cpus, self.num_pools)
        ram_per_pool, ram_remainder = divmod(self.max_ram, self.num_pools)
        
        # initializing different pools ensuring remainders handled
        self.pools: List[ResourcePool] = []
        for i in range(self.num_pools):
            cpu_pool_i = cpu_per_pool + (1 if i < cpu_remainder else 0)
            ram_pool_i = ram_per_pool + (1 if i < ram_remainder else 0)
            new_pool = ResourcePool(i, cpu_pool_i, ram_pool_i, self.rng, **kwargs)
            self.pools.append(new_pool)

    def get_pool_id_with_max_avail_ram(self) -> int:
        max_ram = -1
        id_ = -1
        for i in range(self.num_pools):
            if max_ram < self.pools[i].avail_ram_pool:
                id_ = i
                max_ram = self.pools[i].avail_ram_pool
        return id_

    def num_completed(self) -> int:
        ret = 0
        for pid in range(self.num_pools):
            ret += self.pools[pid].num_completed
        return ret

    def container_tick_times(self) -> List[int]:
        ret = []
        for pid in range(self.num_pools):
            ret.extend(self.pools[pid].container_tick_times)
        return ret

    def run_one_tick(self, suspensions: List[Suspend], 
                     assignments: List[Assignment]) -> List[Failure]:
        '''
        Largely passing through relevant assignments to the pool they belong to. 
        '''
        failures: List[Failure] = []
        for id_ in range(self.num_pools):
            pool_suspensions = [s for s in suspensions if s.pool_id == id_]
            pool_assignments = [a for a in assignments if a.pool_id == id_]
            pool_failures = self.pools[id_].run_one_tick(pool_suspensions, pool_assignments)
            failures.extend(pool_failures)

        return failures
