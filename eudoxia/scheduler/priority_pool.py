from typing import List, Tuple, Dict
import uuid
import logging
from eudoxia.workload import Pipeline, Operator, OperatorState
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.utils import Priority
from .decorators import register_scheduler_init, register_scheduler
from .waiting_queue import WaitingQueueJob

logger = logging.getLogger(__name__)


@register_scheduler_init(key="priority-pool")
def init_priority_pool_scheduler(s):
    s.qry_jobs: List[WaitingQueueJob]   = [] # high
    s.interactive_jobs: List[WaitingQueueJob] = [] # medium 
    s.batch_ppln_jobs: List[WaitingQueueJob]  = [] # low
    
    s.suspending: Dict[uuid.UUID, WaitingQueueJob] = {}
    assert s.executor.num_pools == 2, "Priority-by-pool scheduler requires 2 pools"
    s.oom_failed_to_run = 0

@register_scheduler(key="priority-pool")
def priority_pool_scheduler(s, results: List[ExecutionResult],
                       pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Scheduler that assigns jobs to dedicated pools based on priority.
    Interactive/query jobs go to one pool, batch jobs to another.
    This scheduler doesn't perform preemption/suspension.

    Args:
        results: Execution results from the executor last tick
        pipelines: Newly generated pipelines arriving
    Returns:
        Tuple[List[Suspend], List[Assignment]]:
            - List of containers to suspend (always empty for this scheduler)
            - List of new assignments to provide to Executor
    """
    # Add new pipelines to appropriate queues
    for p in pipelines:
        ops = list(p.values)
        job = WaitingQueueJob(priority=p.priority, p=p, ops=ops)
        if p.priority == Priority.QUERY:
            s.qry_jobs.append(job)
        elif p.priority == Priority.INTERACTIVE:
            s.interactive_jobs.append(job)
        elif p.priority == Priority.BATCH_PIPELINE:
            s.batch_ppln_jobs.append(job)

    # Filter results to get only failures
    failures = [r for r in results if r.failed()]

    for f in failures:
        # Filter out completed operators
        ops = [op for op in f.ops if op.state() != OperatorState.COMPLETED]
        assert ops, "failed container has no incomplete operators"
        # Get pipeline from the first operator
        pipeline = ops[0].pipeline
        job = WaitingQueueJob(priority=f.priority, p=pipeline, ops=ops,
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
            job = WaitingQueueJob(priority=c.priority, p=None,
                                  ops=c.operators, pool_id=pool_id,
                                  container_id=c.container_id, old_ram=c.ram, old_cpu=c.cpu,
                                  error=c.error)
            # c.container_id is UUID type. must use it represented as an int to have
            # it act as key in python dict
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
                                        pool_id=pool_id, priority=job.priority, pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown")
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
                                        pool_id=pool_id, priority=job.priority, pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown")
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
                                    pool_id=pool_id, priority=job.priority, pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown")
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
