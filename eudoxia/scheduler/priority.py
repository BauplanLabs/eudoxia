from typing import List, Tuple, Dict
import uuid
import logging
from eudoxia.workload import Pipeline, Operator
from eudoxia.executor.assignment import Assignment, Failure, Suspend
from eudoxia.utils import Priority
from .decorators import register_scheduler_init, register_scheduler
from .waiting_queue import WaitingQueueJob

logger = logging.getLogger(__name__)


@register_scheduler_init(key="priority")
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

@register_scheduler(key="priority")
def priority_scheduler(s, failures: List[Failure],
                       pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Assign jobs to resources purely in order of priority. This function WILL
    STARVE low priority jobs. 

    Args:
        failures: Jobs failed by the executor last tick
        pipelines: Newly generated pipelines arriving
    Returns:
        Tuple[List[Suspend], List[Assignment]]:
            - List of containers to suspend (for preemption of lower priority jobs)
            - List of new assignments to provide to Executor
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
        # Try to get pipeline from the first operator
        pipeline = None
        if f.ops and len(f.ops) > 0 and f.ops[0].pipeline:
            pipeline = f.ops[0].pipeline
        job = WaitingQueueJob(priority=f.priority, p=pipeline, ops=f.ops,
                              container_id=f.container_id, pool_id=f.pool_id, old_ram=f.ram, old_cpu=f.cpu,
                              error=f.error)
        if f.priority == Priority.QUERY:
            s.qry_jobs.append(job)
        elif f.priority == Priority.INTERACTIVE:
            s.interactive_jobs.append(job)
        elif f.priority == Priority.BATCH_PIPELINE:
            s.batch_ppln_jobs.append(job)

    for pool_id in range(s.executor.num_pools):
        for c in s.executor.pools[pool_id].suspending_containers:
            # Try to get pipeline from the first operator
            pipeline = None
            if c.operators and len(c.operators) > 0 and c.operators[0].pipeline:
                pipeline = c.operators[0].pipeline
            job = WaitingQueueJob(priority=c.priority, p=pipeline,
                                  ops=c.operators, pool_id=pool_id,
                                  container_id=c.container_id, old_ram=c.ram, old_cpu=c.cpu,
                                  error=c.error)
            # Store job by container_id
            s.suspending[c.container_id] = job

    for pool_id in range(s.executor.num_pools):
        for container in s.executor.pools[pool_id].suspended_containers:
            if container.container_id in s.suspending:
                job = s.suspending[container.container_id]
                if job.priority == Priority.QUERY:
                    s.qry_jobs.append(job)
                elif job.priority == Priority.INTERACTIVE:
                    s.interactive_jobs.append(job)
                elif job.priority == Priority.BATCH_PIPELINE:
                    s.batch_ppln_jobs.append(job)
                del s.suspending[container.container_id]

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
                                    priority=job.priority, pool_id=pool_id, 
                                    pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown_pipeline")
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
                                    priority=job.priority, pool_id=pool_id, 
                                    pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown_pipeline")
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
                                    priority=job.priority, pool_id=pool_id, 
                                    pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown_pipeline")
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
            susobj = Suspend(sus.container_id, sus.pool_id)
            suspensions.append(susobj)

    return suspensions, new_assignments
