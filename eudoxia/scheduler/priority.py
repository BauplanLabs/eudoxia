from typing import List, Tuple, Dict
import uuid
import logging
from eudoxia.workload import Pipeline, Operator, OperatorState
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.utils import Priority
from .decorators import register_scheduler_init, register_scheduler
from .waiting_queue import WaitingQueueJob, RetryStats

logger = logging.getLogger(__name__)


@register_scheduler_init(key="priority")
def init_priority_scheduler(s):
    s.multi_operator_containers = s.params["multi_operator_containers"]

    s.qry_jobs: List[WaitingQueueJob]   = [] # high
    s.interactive_jobs: List[WaitingQueueJob] = [] # medium 
    s.batch_ppln_jobs: List[WaitingQueueJob]  = [] # low
    s.queues_by_prio = {
        Priority.QUERY: s.qry_jobs,
        Priority.INTERACTIVE: s.interactive_jobs,
        Priority.BATCH_PIPELINE: s.batch_ppln_jobs,
    }

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
def priority_scheduler(s, results: List[ExecutionResult],
                       pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Assign jobs to resources purely in order of priority. This function WILL
    STARVE low priority jobs.

    Args:
        results: Execution results from the executor last tick
        pipelines: Newly generated pipelines arriving
    Returns:
        Tuple[List[Suspend], List[Assignment]]:
            - List of containers to suspend (for preemption of lower priority jobs)
            - List of new assignments to provide to Executor
    """
    # Collect all pipelines that need processing: new arrivals + pipelines with status changes
    pipelines_to_process = {p.pipeline_id: p for p in pipelines}
    for r in results:
        for op in r.ops:
            pipelines_to_process[op.pipeline.pipeline_id] = op.pipeline

    # Build retry info from failed results
    retry_info = {}
    for r in results:
        if r.failed():
            for op in r.ops:
                if op.state() != OperatorState.COMPLETED:
                    retry_info[op.id] = RetryStats(
                        old_ram=r.ram,
                        old_cpu=r.cpu,
                        error=r.error,
                        container_id=r.container_id,
                        pool_id=r.pool_id,
                    )

    # Queue new work as necessary
    if pipelines_to_process:
        # identify work already queued (so we can avoid double queueing later)
        already_queued = set()
        for queue in s.queues_by_prio.values():
            for job in queue:
                for op in job.ops:
                    already_queued.add(op.id)

        # break ready, non-queued work into distinct jobs, based on multi_operator_containers settings
        jobs = []
        for pipeline in pipelines_to_process.values():
            # identify any ops we need to queue now based pipeline arrivals/changes
            if s.multi_operator_containers:
                op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
            else:
                op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            op_list = [op for op in op_list if op.id not in already_queued]
            if len(op_list) == 0:
                continue
            if s.multi_operator_containers:
                # For multi-op, use retry info from first op if any
                retry_stats = retry_info.get(op_list[0].id)
                jobs.append(WaitingQueueJob(priority=pipeline.priority, p=pipeline, ops=op_list, retry_stats=retry_stats))
            else:
                for op in op_list:
                    retry_stats = retry_info.get(op.id)
                    jobs.append(WaitingQueueJob(priority=pipeline.priority, p=pipeline, ops=[op], retry_stats=retry_stats))

        # select queue for job based on priority
        for job in jobs:
            s.queues_by_prio[job.pipeline.priority].append(job)

    # suspending containers
    for pool_id in range(s.executor.num_pools):
        for c in s.executor.pools[pool_id].suspending_containers:
            # Filter out completed operators
            ops = [op for op in c.operators if op.state() != OperatorState.COMPLETED]
            assert ops, "suspending container has no incomplete operators"

            # Get pipeline from the first operator
            pipeline = ops[0].pipeline
            retry_stats = RetryStats(
                old_ram=c.assignment.ram,
                old_cpu=c.assignment.cpu,
                error=c.error,
                container_id=c.container_id,
                pool_id=pool_id,
            )
            job = WaitingQueueJob(priority=c.priority, p=pipeline, ops=ops, retry_stats=retry_stats)
            # Store job by container_id
            s.suspending[c.container_id] = job
    for pool_id in range(s.executor.num_pools):
        for container in s.executor.pools[pool_id].suspended_containers:
            if container.container_id in s.suspending:
                job = s.suspending.pop(container.container_id)
                s.queues_by_prio[job.priority].append(job)

    # resource stats per pool/machine
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
            op_list = job.ops

            rs = job.retry_stats
            # is this job placed in the queue because it previously failed,
            # i.e. OOM error
            if rs is not None and rs.error is not None:
                job_cpu = 2 * rs.old_cpu
                job_ram = 2 * rs.old_ram
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
            elif rs is not None and (rs.old_cpu < pool_stats[pool_id]["avail_cpu"] and
                                     rs.old_ram < pool_stats[pool_id]["avail_ram"]):
                job_cpu = rs.old_cpu
                job_ram = rs.old_ram
                asgmnt = Assignment(ops=op_list, cpu=job_cpu, ram=job_ram,
                                    priority=job.priority, pool_id=pool_id,
                                    pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown_pipeline")
                pool_stats[pool_id]["avail_cpu"] -= job_cpu
                pool_stats[pool_id]["avail_ram"] -= job_ram
                to_start.append(asgmnt)
            # The case for newly arrived jobs
            else:
                job_cpu = max(1, int(pool_stats[pool_id]["total_cpu"] / 10))
                job_ram = max(1, int(pool_stats[pool_id]["total_ram"] / 10))
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
