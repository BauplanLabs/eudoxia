from typing import List, Tuple
from eudoxia.utils import *
import uuid
import logging
logger = logging.getLogger(__name__)

class WaitingQueueJob: 
    def __init__(self, priority: Priority, p: Pipeline=None, ops:
                 List[Operator]=None, pool_id: int = None,
                 cid: uuid.UUID = None, old_ram: int = None, old_cpu: int =
                 None, error: str = None): 
        self.priority = priority
        self.pipeline = p
        self.ops = ops
        self.pool_id = pool_id
        self.cid = cid
        self.old_ram = old_ram
        self.old_cpu = old_cpu
        self.error = error


def naive_pipeline_init(s): 
    s.waiting_queue: Tuple[List[Operator], Priority] = []

def naive_pipeline(s, failures: List[Failure], 
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    A naive implementation of a scheduling pipeline which allocates all
    resources of a pool to a single pipeline and handles them in a FIFO order. It
    assigns one job at a time to each pool created.
    Args:
        pipelines(list): passed in list from WorkloadGenerator
    Returns:
        list[assignment]: the resulting assignment to provide to Executor
    """
    for p in pipelines:
        ops = [op for op in p.values]
        s.waiting_queue.append((ops, p.priority))
    if len(s.waiting_queue) == 0:
        return []

    suspensions = []
    assignments = []
    for pool_id in range(s.executor.num_pools):
        avail_cpu_pool = s.executor.pools[pool_id].avail_cpu_pool
        avail_ram_pool = s.executor.pools[pool_id].avail_ram_pool
        if avail_cpu_pool > 0 and avail_ram_pool > 0:
            op_list, priority = s.waiting_queue.pop(0)
            assignment = Assignment(ops=op_list, cpu=avail_cpu_pool, ram=avail_ram_pool,
                                    priority=priority, pool_id=pool_id)
            assignments.append(assignment)
    return [], assignments

def init_priority_pool_scheduler(s):
    s.qry_jobs: List[WaitingQueueJob]   = [] # high
    s.interactive_jobs: List[WaitingQueueJob] = [] # medium 
    s.batch_ppln_jobs: List[WaitingQueueJob]  = [] # low
    
    s.suspending: Dict[uuid.UUID, WaitingQueueJob] = {}
    assert s.executor.num_pools == 2, "Priority-by-pool scheduler requires 2 pools"
    s.oom_failed_to_run = 0

def priority_pool_scheduler(s, failures: List[Failure],
                       pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    '''
    jobs come in, we don't suspend anything, we send interactive/queries to
    one, batch to another. 
    '''
    # Add new pipelines to appropriate queues
    for p in pipelines:
        job = WaitingQueueJob(priority=p.priority, p=p)
        if p.priority == Priority.QUERY:
            s.qry_jobs.append(job)
        elif p.priority == Priority.INTERACTIVE:
            s.interactive_jobs.append(job)
        elif p.priority == Priority.BATCH_PIPELINE:
            s.batch_ppln_jobs.append(job)

    for f in failures:
        job = WaitingQueueJob(priority=f.priority, p=None, ops=f.ops,
                              cid=f.cid, pool_id=f.pool_id, old_ram=f.ram, old_cpu=f.cpu,
                              error=f.error)
        if f.priority == Priority.QUERY:
            s.qry_jobs.append(job)
        elif f.priority == Priority.INTERACTIVE:
            s.interactive_jobs.append(job)
        elif f.priority == Priority.BATCH_PIPELINE:
            s.batch_ppln_jobs.append(job)

    for pool_id in range(s.executor.num_pools):
        for c in s.executor.pools[pool_id].suspending_containers:
            job = WaitingQueueJob(priority=c.priority, p=None,
                                  ops=c.operators, pool_id=pool_id,
                                  cid=c.cid, old_ram=c.ram, old_cpu=c.cpu,
                                  error=c.error)
            # c.cid is UUID type. must use it represented as an int to have
            # it act as key in python dict
            s.suspending[c.cid] = job

    for pool_id in range(s.executor.num_pools):
        for container in s.executor.pools[pool_id].suspended_containers:
            if container.cid in s.suspending:
                job = s.suspending[container.cid]
                if job.priority == Priority.QUERY:
                    s.qry_jobs.append(job)
                elif job.priority == Priority.INTERACTIVE:
                    s.interactive_jobs.append(job)
                elif job.priority == Priority.BATCH_PIPELINE:
                    s.batch_ppln_jobs.append(job)
                del s.suspending[container.cid]

    pool_stats = {}
    for i in range(s.executor.num_pools):
        # Current available resources to ensure we do not overallocate
        avail_cpu_pool = s.executor.pools[i].avail_cpu_pool
        avail_ram_pool = s.executor.pools[i].avail_ram_pool
        # Maximum pool size to calculate how much to provide
        total_cpu_pool = s.executor.pools[i].max_cpu_pool
        total_ram_pool = s.executor.pools[i].max_ram_pool

        # track current resource values for each pool
        pool_stats[i] = {
                          "avail_cpu": avail_cpu_pool,
                          "avail_ram": avail_ram_pool, 
                          "total_cpu": total_cpu_pool,
                          "total_ram": total_ram_pool, 
                        }

    # Defines what priorirty levels are assigned to which pools
    pool_queues = {0: [s.qry_jobs, s.interactive_jobs], 
                   1: [s.batch_ppln_jobs]}
    new_assignments = []

    for pool_id in range(s.executor.num_pools): 
        queues = pool_queues[pool_id]
        for queue in queues:
            # In descending order of priority, and while there are resources
            # available to allocate, take 10% of available CPU and RAM and allocate
            # a container for the current job. Jobs are ordered by time of arrival
            # Must give at minimum 1 GB RAM and 1CPU
            to_remove = []
            to_start = []
            for job in queue:
                avail_ram = pool_stats[pool_id]["avail_ram"] 
                avail_cpu = pool_stats[pool_id]["avail_cpu"] 

                # the pool is depleted so we shouldn't make any allocations
                if avail_ram == 0 or avail_cpu == 0:
                    assert avail_ram == 0 and avail_cpu == 0, f"Avail RAM: {avail_ram}, CPU: {avail_cpu} invalid pool {pool_id}"
                    break
                
                logger.info(pool_stats[pool_id])
                to_remove.append(job)
                if job.pipeline is not None:
                    op_list = [op for op in job.pipeline.values]
                else: 
                    op_list = job.ops


                # i.e. OOM error
                if job.error is not None:
                    logger.info("OOM")
                    job_cpu = 2*job.old_cpu
                    job_ram = 2*job.old_ram
                    cpu_ratio = job_cpu / pool_stats[pool_id]["total_cpu"]
                    ram_ratio = job_ram / pool_stats[pool_id]["total_ram"]
                    if cpu_ratio >= 0.5 or ram_ratio >= 0.5:
                        s.oom_failed_to_run += 1
                        continue

                    if job_cpu >= pool_stats[pool_id]["avail_cpu"] or job_ram >= pool_stats[pool_id]["avail_ram"]:
                        job_cpu = pool_stats[pool_id]["avail_cpu"]
                        job_ram = pool_stats[pool_id]["avail_ram"]
                    asgmnt = Assignment(ops=op_list, cpu=job_cpu, ram=job_ram,
                                        pool_id=pool_id, priority=job.priority)
                    pool_stats[pool_id]["avail_cpu"] -= job_cpu
                    pool_stats[pool_id]["avail_ram"] -= job_ram
                    to_start.append(asgmnt)
                # If old_cpu is not None, this job was run previously. However,
                # if it is here, it didn't have an error. Thus it must've been
                # pre-empted earlier by a higher priority job
                elif job.old_cpu is not None and (job.old_cpu <= pool_stats[pool_id]["avail_cpu"] and
                                                job.old_ram <= pool_stats[pool_id]["avail_ram"]):
                    logger.info("PREEMPT")
                    job_cpu = job.old_cpu
                    job_ram = job.old_ram
                    # can be == instead of >= as we checked <= in elif 
                    if job_cpu == pool_stats[pool_id]["avail_cpu"] or job_ram == pool_stats[pool_id]["avail_ram"]:
                        job_cpu = pool_stats[pool_id]["avail_cpu"]
                        job_ram = pool_stats[pool_id]["avail_ram"]
                    asgmnt = Assignment(ops=op_list, cpu=job_cpu, ram=job_ram,
                                        pool_id=pool_id, priority=job.priority)
                    pool_stats[pool_id]["avail_cpu"] -= job_cpu
                    pool_stats[pool_id]["avail_ram"] -= job_ram
                    to_start.append(asgmnt)
                # The case for newly arrived jobs
                else:
                    logger.info("NEW")
                    job_cpu = int(pool_stats[pool_id]["total_cpu"] / 10)
                    job_ram = int(pool_stats[pool_id]["total_ram"] / 10)
                    if job_cpu >= pool_stats[pool_id]["avail_cpu"] or job_ram >= pool_stats[pool_id]["avail_ram"]:
                        job_cpu = pool_stats[pool_id]["avail_cpu"]
                        job_ram = pool_stats[pool_id]["avail_ram"]
                    asgmnt = Assignment(ops=op_list, cpu=job_cpu, ram=job_ram,
                                    pool_id=pool_id, priority=job.priority)
                    pool_stats[pool_id]["avail_cpu"] -= job_cpu
                    pool_stats[pool_id]["avail_ram"] -= job_ram
                    to_start.append(asgmnt)
            for j in to_start:
                new_assignments.append(j)
            for j in to_remove:
                queue.remove(j)

    suspensions = []
    # Print diagnostic information about what assignments are created. This
    # string is formatted via the Assignment __repr__ function
    for a in new_assignments:
        logger.info(a)
    return suspensions, new_assignments



def init_priority_scheduler(s): 
    s.qry_jobs: List[WaitingQueueJob]   = [] # high
    s.interactive_jobs: List[WaitingQueueJob] = [] # medium 
    s.batch_ppln_jobs: List[WaitingQueueJob]  = [] # low

    s.suspending: Dict[uuid.UUID, WaitingQueueJob] = {}
    s.oom_failed_to_run = 0

def get_pool_with_max_avail_ram(s, pool_stats):
    id_ = -1
    max_ram = 0 # must be 0 to ensure we get pool with greatest, nonzero amount of ram

    # pick pool with largest avail RAM and non-zero number of avail CPUs
    for i in range(s.executor.num_pools):
        if (pool_stats[i]["avail_cpu"] > 0) and (max_ram < pool_stats[i]["avail_ram"]):
            id_ = i
            max_ram = pool_stats[i]["avail_ram"]
    return id_

def priority_scheduler(s, failures: List[Failure],
                       pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Assign jobs to resources purely in order of priority. This function WILL
    STARVE low priority jobs. 

    Args:
        failures are a jobs failed by the executor last tick
        pipelines are newly generated pipelines arriving
    Returns:
        suspensions: List[Suspend]
    """
    # Add new pipelines to appropriate queues
    for p in pipelines:
        job = WaitingQueueJob(priority=p.priority, p=p)
        if p.priority == Priority.QUERY:
            s.qry_jobs.append(job)
        elif p.priority == Priority.INTERACTIVE:
            s.interactive_jobs.append(job)
        elif p.priority == Priority.BATCH_PIPELINE:
            s.batch_ppln_jobs.append(job)

    for f in failures:
        job = WaitingQueueJob(priority=f.priority, p=None, ops=f.ops,
                              cid=f.cid, pool_id=f.pool_id, old_ram=f.ram, old_cpu=f.cpu,
                              error=f.error)
        if f.priority == Priority.QUERY:
            s.qry_jobs.append(job)
        elif f.priority == Priority.INTERACTIVE:
            s.interactive_jobs.append(job)
        elif f.priority == Priority.BATCH_PIPELINE:
            s.batch_ppln_jobs.append(job)

    for pool_id in range(s.executor.num_pools):
        for c in s.executor.pools[pool_id].suspending_containers:
            job = WaitingQueueJob(priority=c.priority, p=None,
                                  ops=c.operators, pool_id=pool_id,
                                  cid=c.cid, old_ram=c.ram, old_cpu=c.cpu,
                                  error=c.error)
            # c.cid is UUID type. must use it represented as an int to have
            # it act as key in python dict
            s.suspending[c.cid] = job

    for pool_id in range(s.executor.num_pools):
        for container in s.executor.pools[pool_id].suspended_containers:
            if container.cid in s.suspending:
                job = s.suspending[container.cid]
                if job.priority == Priority.QUERY:
                    s.qry_jobs.append(job)
                elif job.priority == Priority.INTERACTIVE:
                    s.interactive_jobs.append(job)
                elif job.priority == Priority.BATCH_PIPELINE:
                    s.batch_ppln_jobs.append(job)
                del s.suspending[container.cid]

    pool_stats = {}
    for i in range(s.executor.num_pools):
        # Current available resources to ensure we do not overallocate
        avail_cpu_pool = s.executor.pools[i].avail_cpu_pool
        avail_ram_pool = s.executor.pools[i].avail_ram_pool
        # Maximum pool size to calculate how much to provide
        total_cpu_pool = s.executor.pools[i].max_cpu_pool
        total_ram_pool = s.executor.pools[i].max_ram_pool

        # track current resource values for each pool
        pool_stats[i] = {
                          "avail_cpu": avail_cpu_pool,
                          "avail_ram": avail_ram_pool, 
                          "total_cpu": total_cpu_pool,
                          "total_ram": total_ram_pool, 
                        }

    # order of these queues is VERY IMPORTANT: must go in order of
    # DESCENDING priority
    queues = [s.qry_jobs, s.interactive_jobs, s.batch_ppln_jobs]
    new_assignments = []

    for queue in queues:
        # In descending order of priority, and while there are resources
        # available to allocate, take 10% of available CPU and RAM and allocate
        # a container for the current job. Jobs are ordered by time of arrival
        # Must give at minimum 1 GB RAM and 1CPU
        to_remove = []
        to_start = []
        for job in queue:
            # checking which pool has the most available ram. MUST USE
            # COPIED STATS as scheduler is placing assignments into pools  
            # TODO: PARAMATERIZE THIS
            pool_id = get_pool_with_max_avail_ram(s, pool_stats)
            # all pools depleted
            if pool_id == -1:
                break
            print(pool_stats)

            to_remove.append(job)
            if job.pipeline is not None:
                op_list = [op for op in job.pipeline.values]
            else: 
                op_list = job.ops

            # is this job placed in the queue because it previously failed,
            # i.e. OOM error
            if job.error is not None:
                job_cpu = 2*job.old_cpu
                job_ram = 2*job.old_ram
                if job_cpu > pool_stats[pool_id]["avail_cpu"] or job_ram > pool_stats[pool_id]["avail_ram"]:
                    continue
                cpu_ratio = job_cpu / pool_stats[pool_id]["total_cpu"]
                ram_ratio = job_ram / pool_stats[pool_id]["total_ram"]
                if cpu_ratio >= 0.5 or ram_ratio >= 0.5:
                    s.oom_failed_to_run += 1
                    continue
                asgmnt = Assignment(ops=op_list, cpu=job_cpu, ram=job_ram,
                                    pool_id=pool_id, priority=job.priority)
                pool_stats[pool_id]["avail_cpu"] -= job_cpu
                pool_stats[pool_id]["avail_ram"] -= job_ram
                to_start.append(asgmnt)
            # If old_cpu is not None, this job was run previously. However,
            # if it is here, it didn't have an error. Thus it must've been
            # pre-empted earlier by a higher priority job
            elif job.old_cpu is not None and (job.old_cpu < pool_stats[pool_id]["avail_cpu"] and
                                            job.old_ram < pool_stats[pool_id]["avail_ram"]):
                job_cpu = job.old_cpu
                job_ram = job.old_ram
                asgmnt = Assignment(ops=op_list, cpu=job_cpu, ram=job_ram,
                                    pool_id=pool_id, priority=job.priority)
                pool_stats[pool_id]["avail_cpu"] -= job_cpu
                pool_stats[pool_id]["avail_ram"] -= job_ram
                to_start.append(asgmnt)
            # The case for newly arrived jobs
            else:
                job_cpu = int(pool_stats[pool_id]["total_cpu"] / 10)
                job_ram = int(pool_stats[pool_id]["total_ram"] / 10)
                if job_cpu >= pool_stats[pool_id]["avail_cpu"] or job_ram >= pool_stats[pool_id]["avail_ram"]:
                    job_cpu = pool_stats[pool_id]["avail_cpu"]
                    job_ram = pool_stats[pool_id]["avail_ram"]
                asgmnt = Assignment(ops=op_list, cpu=job_cpu, ram=job_ram,
                                pool_id=pool_id, priority=job.priority)
                pool_stats[pool_id]["avail_cpu"] -= job_cpu
                pool_stats[pool_id]["avail_ram"] -= job_ram
                to_start.append(asgmnt)
        for j in to_start:
            new_assignments.append(j)
        for j in to_remove:
            queue.remove(j)

    suspensions = []
    if len(s.qry_jobs) > 0:
        # If there are jobs in high priority queues then check
        # if any containers are preemptible and make a command to suspend
        # them. When the resources are free, the first part of this
        # scheduling algorithm will assign them to appropriate jobs
        num_to_suspend = len(s.qry_jobs)
        cnt = 0
        to_suspend = []

        pool_id = 0
        exhausted = [False for _ in range(s.executor.num_pools)]
        iters = [iter(s.executor.pools[i].active_containers) for i in range(s.executor.num_pools)]
        while cnt < num_to_suspend:
            if all(exhausted):
                break
            try: 
                container = next(iters[pool_id])
                while container.priority == Priority.QUERY:
                    container = next(iters[pool_id])
                if container.can_suspend_container():
                    cnt += 1
                    to_suspend.append(container)
            except StopIteration:
                exhausted[pool_id] = True
            pool_id = ((pool_id + 1) % s.executor.num_pools)

        for sus in to_suspend:
            susobj = Suspend(sus.cid, sus.pool_id)
            suspensions.append(susobj)
    # Print diagnostic information about what assignments are created. This
    # string is formatted via the Assignment __repr__ function
    for a in new_assignments:
        logger.info(a)
    return suspensions, new_assignments


''' The table of pre-implemented scheduling algorithms and corresponding
initialization functions'''
INIT_ALGOS = {'naive': naive_pipeline_init, 'priority': init_priority_scheduler, 
              'priority-pool': init_priority_pool_scheduler}
SCHEDULING_ALGOS = {'naive': naive_pipeline, 'priority': priority_scheduler, 
                    'priority-pool': priority_pool_scheduler}


def register_scheduler_init(key):
    def decorator(func):
        if key in INIT_ALGOS: 
            raise KeyError(f"Algorithm key '{key}' is already registered.")
        INIT_ALGOS[key] = func
        return func
    return decorator

def register_scheduler(key):
    def decorator(func):
        if key in SCHEDULING_ALGOS: 
            raise KeyError(f"Algorithm key '{key}' is already registered.")
        SCHEDULING_ALGOS[key] = func
        return func
    return decorator

