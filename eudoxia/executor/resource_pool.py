import logging
import uuid
import numpy as np
from typing import List
from eudoxia.workload import Assignment, Suspend, Failure
from .container import Container

logger = logging.getLogger(__name__)


class ResourcePool:
    """
    Manager of a pool of resources and active containers, the Executor takes
    assignments and ensures that all costs and resources are accounted for and
    additional are allocated if instructed.
    """
    def __init__(self, pool_id, cpu_pool, ram_pool, rng, tick_length_secs, **kwargs):
        self.pool_id = pool_id
        self.max_cpu_pool = cpu_pool
        self.max_ram_pool = ram_pool
        self.avail_cpu_pool = cpu_pool
        self.avail_ram_pool = ram_pool
        self.rng = rng
        self.tick_length_secs = tick_length_secs

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
            secs_left = c._num_ticks_left * self.tick_length_secs 
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
                                      prty=a.priority, pool_id=self.pool_id, rng=self.rng,
                                      tick_length_secs=self.tick_length_secs)
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