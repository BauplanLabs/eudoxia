import logging
from typing import List
from .assignment import Assignment, Suspend, ExecutionResult
from .container import Container

logger = logging.getLogger(__name__)


class ResourcePool:
    """
    Manager of a pool of resources and active containers, the Executor takes
    assignments and ensures that all costs and resources are accounted for and
    additional are allocated if instructed.

    A resource pool is analogous to a machine on which we can run containers.
    """
    def __init__(self, pool_id, cpu_pool, ram_pool, ticks_per_second, multi_operator_containers=True, **kwargs):
        self.pool_id = pool_id
        self.max_cpu_pool = cpu_pool
        self.max_ram_pool = ram_pool
        self.avail_cpu_pool = cpu_pool
        self.avail_ram_pool = ram_pool
        self.ticks_per_second = ticks_per_second
        self.tick_length_secs = 1.0 / ticks_per_second
        self.multi_operator_containers = multi_operator_containers

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

    def get_container_by_id(self, container_id: str) -> Container: 
        for container in self.active_containers:
            if container.container_id == container_id:
                return container
        return None

    def verify_valid_suspend(self, suspensions: List[Suspend]): 
        """
        ensure suspension is valid
        """
        for s in suspensions:
            container = self.get_container_by_id(s.container_id)
            assert container.can_suspend_container(), "Container cannot be suspended right now"

    def status_report(self):
        logger.info(f"----------STATUS REPORT POOL {self.pool_id}----------")
        for c in self.active_containers:
            secs_left = c._num_ticks_left * self.tick_length_secs 
            logger.info(f"{c.container_id} running with {c._num_ticks_left} ticks left or {secs_left} seconds left")
        logger.info(f"----------END STATUS REPORT FOR POOL {self.pool_id}----------")

    def log_pool_utilization(self): 
        cpu_util = 100.0 * self.avail_cpu_pool / self.max_cpu_pool
        ram_util = 100.0 * self.avail_ram_pool / self.max_ram_pool
        out_line = f"{self.i}, {cpu_util:.2f}, {ram_util:.2f}\n"
        self.outfile.write(out_line)

    def run_one_tick(self, suspensions: List[Suspend],
                     assignments: List[Assignment]) -> List[ExecutionResult]:
        """
        Run a single tick for the executor, decrement remaining ticks for all
        active containers, remove completed ones, and update relevant
        statistics.
        """
        self.i += 1
        if len(suspensions) > 0:
            self.verify_valid_suspend(suspensions)
            for s in suspensions:
                container = self.get_container_by_id(s.container_id)
                container.suspend_container()
                self.suspending_containers.append(container)
                self.active_containers.remove(container)
        
        results = []
        if len(assignments) > 0:
            self.verify_valid_assignment(assignments)
            for a in assignments:
                # Validate multi-operator containers
                if not self.multi_operator_containers and len(a.ops) > 1:
                    result = ExecutionResult(
                        ops=a.ops,
                        cpu=a.cpu,
                        ram=a.ram,
                        priority=a.priority,
                        pool_id=self.pool_id,
                        container_id=None,
                        error="multi"
                    )
                    results.append(result)
                    logger.error(f"Assignment validation failed: multiple operators not allowed")
                    continue

                # Create container (transitions ops to ASSIGNED, then RUNNING as they execute)
                logger.info(f"start container ram={a.ram} cpu={a.cpu} ops={len(a.ops)} priority={a.priority} pool_id={self.pool_id} pipeline_id={a.pipeline_id}")
                container = Container(ram=a.ram, cpu=a.cpu, ops=a.ops,
                                      prty=a.priority, pool_id=self.pool_id,
                                      ticks_per_second=self.ticks_per_second)
                self.avail_cpu_pool -= a.cpu
                self.avail_ram_pool -= a.ram
                self.active_containers.append(container)

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
        for c in self.active_containers:
            c.tick()
            if c.is_completed():
                self.avail_cpu_pool += c.cpu
                self.avail_ram_pool += c.ram
                to_remove.append(c)

                if c.error is None:
                    self.num_completed += 1

                # Create an ExecutionResult for every completed container
                result = ExecutionResult(ops=c.operators, cpu=c.cpu, ram=c.ram,
                                        priority=c.priority, pool_id=c.pool_id,
                                        container_id=c.container_id, error=c.error)
                logger.info(result)
                results.append(result)

                # TODO: if we're computing p99 latency on this, is it
                # better to include all containers, or only those that
                # ran successfully?
                self.container_tick_times.append(c.ticks_elapsed())

        for c in to_remove:
            self.active_containers.remove(c)

        self.log_pool_utilization()
        return results
