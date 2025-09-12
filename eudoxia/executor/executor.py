import logging
from typing import List
from .assignment import Suspend, Assignment, Failure
from .resource_pool import ResourcePool

logger = logging.getLogger(__name__)


class Executor:
    """
    Manager of a pool of resources and active containers, the Executor takes
    assignments and ensures that all costs and resources are accounted for and
    additional are allocated if instructed.
    """
    def __init__(self, num_pools, cpu_pool, ram_pool, rng, ticks_per_second, **kwargs):
        # total amount of resources allocated
        self.num_pools = num_pools
        self.max_cpus = cpu_pool
        self.max_ram = ram_pool
        self.rng = rng
        self.ticks_per_second = ticks_per_second
        self.tick_length_secs = 1.0 / ticks_per_second
        
        # computation for dividing ram, cpu among diff pools
        cpu_per_pool, cpu_remainder = divmod(self.max_cpus, self.num_pools)
        ram_per_pool, ram_remainder = divmod(self.max_ram, self.num_pools)
        
        # initializing different pools ensuring remainders handled
        self.pools: List[ResourcePool] = []
        for i in range(self.num_pools):
            cpu_pool_i = cpu_per_pool + (1 if i < cpu_remainder else 0)
            ram_pool_i = ram_per_pool + (1 if i < ram_remainder else 0)
            new_pool = ResourcePool(pool_id=i, cpu_pool=cpu_pool_i, ram_pool=ram_pool_i, 
                                   rng=self.rng, ticks_per_second=self.ticks_per_second, **kwargs)
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
