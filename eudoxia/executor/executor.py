import logging
from typing import List
from .assignment import Suspend, Assignment, ExecutionResult
from .resource_pool import ResourcePool

logger = logging.getLogger(__name__)


class Executor:
    """
    Manager of a pool of resources and active containers, the Executor takes
    assignments and ensures that all costs and resources are accounted for and
    additional are allocated if instructed.
    
    Acts like a cluster manager that keeps track of utilization of machines
    (that is, resource pools).
    """
    def __init__(self, num_pools, cpus_per_pool, ram_gb_per_pool, ticks_per_second,
                 allow_memory_overcommit=False, **kwargs):
        self.num_pools = num_pools
        self.cpus_per_pool = cpus_per_pool
        self.ram_gb_per_pool = ram_gb_per_pool
        self.ticks_per_second = ticks_per_second
        self.tick_length_secs = 1.0 / ticks_per_second

        # Initialize pools with identical resources
        self.pools: List[ResourcePool] = []
        for i in range(self.num_pools):
            new_pool = ResourcePool(pool_id=i, cpu_pool=cpus_per_pool, ram_pool=ram_gb_per_pool,
                                   ticks_per_second=self.ticks_per_second,
                                   allow_memory_overcommit=allow_memory_overcommit, **kwargs)
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

    def get_total_ram_gb(self) -> float:
        """Return total RAM capacity across all pools in GB."""
        return self.num_pools * self.ram_gb_per_pool

    def get_allocated_ram_gb(self) -> float:
        """Return total allocated RAM across all pools in GB."""
        return sum(p.get_allocated_ram_gb() for p in self.pools)

    def get_consumed_ram_gb(self) -> float:
        """Return total consumed RAM across all pools in GB."""
        return sum(p.get_consumed_ram_gb() for p in self.pools)

    def run_one_tick(self, suspensions: List[Suspend],
                     assignments: List[Assignment]) -> List[ExecutionResult]:
        '''
        Largely passing through relevant assignments to the pool they belong to.
        '''
        results: List[ExecutionResult] = []
        for id_ in range(self.num_pools):
            pool_suspensions = [s for s in suspensions if s.pool_id == id_]
            pool_assignments = [a for a in assignments if a.pool_id == id_]
            pool_results = self.pools[id_].run_one_tick(pool_suspensions, pool_assignments)
            results.extend(pool_results)

        return results
