"""
Priority-Estimate Scheduler — same as 'priority', but uses op.estimate.mem_peak_gb_est
to set the initial RAM allocation for newly-arrived jobs.

When the noisyoracle estimator is active, this avoids OOM retries by allocating
the right amount of RAM on the first attempt.  Falls back to the fixed 10%-of-pool
heuristic when no estimate is available (estimator_algo=None or empty estimate fields).

Key difference from 'priority':
  - Newly-arrived jobs: job_ram = ceil(max(mem_peak_gb_est across ops))  if available
                                  10% of pool                             otherwise
  - OOM retry / preempt-resume:   unchanged (doubles RAM as before)
  - Queue ordering, preemption:   unchanged
"""
import math
import logging
from typing import List, Tuple, Dict
import uuid

from eudoxia.workload import Pipeline, Operator, OperatorState
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.utils import Priority
from .decorators import register_scheduler_init, register_scheduler
from .waiting_queue import WaitingQueueJob, RetryStats
from .priority import get_pool_with_max_avail_ram   # reuse helper

logger = logging.getLogger(__name__)


def _estimate_ram(op_list) -> int | None:
    """
    Extract RAM estimate from op.estimate for a list of ops.

    For a multi-op container, ops run sequentially, so peak memory is
    max across ops.

    Returns:
        int: ceil(max(mem_peak_gb_est)) if all ops have an estimate, else None.
    """
    hints = [op.estimate.mem_peak_gb_est for op in op_list]
    if any(h is None for h in hints):
        return None
    return max(1, math.ceil(max(hints)))


@register_scheduler_init(key="priority-est")
def init_priority_est_scheduler(s):
    s.multi_operator_containers = s.params["multi_operator_containers"]

    s.qry_jobs: List[WaitingQueueJob]        = []
    s.interactive_jobs: List[WaitingQueueJob]= []
    s.batch_ppln_jobs: List[WaitingQueueJob] = []
    s.queues_by_prio = {
        Priority.QUERY:          s.qry_jobs,
        Priority.INTERACTIVE:    s.interactive_jobs,
        Priority.BATCH_PIPELINE: s.batch_ppln_jobs,
    }

    s.suspending: Dict[uuid.UUID, WaitingQueueJob] = {}
    s.oom_failed_to_run = 0


@register_scheduler(key="priority-est")
def priority_est_scheduler(s, results: List[ExecutionResult],
                            pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority scheduler that uses mem_peak_gb_est from op.estimate to allocate
    the right amount of RAM on first assignment, eliminating unnecessary OOM retries.

    Falls back to the fixed 10%-of-pool heuristic when estimate is unavailable.
    """
    # ── queue management (identical to priority) ──────────────────────────────
    pipelines_to_process = {p.pipeline_id: p for p in pipelines}
    for r in results:
        for op in r.ops:
            pipelines_to_process[op.pipeline.pipeline_id] = op.pipeline

    retry_info = {}
    for r in results:
        if r.failed():
            for op in r.ops:
                if op.state() != OperatorState.COMPLETED:
                    retry_info[op.id] = RetryStats(
                        old_ram=r.ram, old_cpu=r.cpu, error=r.error,
                        container_id=r.container_id, pool_id=r.pool_id,
                    )

    if pipelines_to_process:
        already_queued = set()
        for queue in s.queues_by_prio.values():
            for job in queue:
                for op in job.ops:
                    already_queued.add(op.id)

        jobs = []
        for pipeline in pipelines_to_process.values():
            if s.multi_operator_containers:
                op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
            else:
                op_list = pipeline.runtime_status().get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            op_list = [op for op in op_list if op.id not in already_queued]
            if not op_list:
                continue
            if s.multi_operator_containers:
                retry_stats = retry_info.get(op_list[0].id)
                jobs.append(WaitingQueueJob(priority=pipeline.priority, p=pipeline,
                                            ops=op_list, retry_stats=retry_stats))
            else:
                for op in op_list:
                    retry_stats = retry_info.get(op.id)
                    jobs.append(WaitingQueueJob(priority=pipeline.priority, p=pipeline,
                                                ops=[op], retry_stats=retry_stats))

        for job in jobs:
            s.queues_by_prio[job.pipeline.priority].append(job)

    for pool_id in range(s.executor.num_pools):
        for c in s.executor.pools[pool_id].suspending_containers:
            ops = [op for op in c.operators if op.state() != OperatorState.COMPLETED]
            assert ops
            pipeline = ops[0].pipeline
            retry_stats = RetryStats(old_ram=c.assignment.ram, old_cpu=c.assignment.cpu,
                                     error=c.error, container_id=c.container_id, pool_id=pool_id)
            job = WaitingQueueJob(priority=c.priority, p=pipeline, ops=ops, retry_stats=retry_stats)
            s.suspending[c.container_id] = job
    for pool_id in range(s.executor.num_pools):
        for container in s.executor.pools[pool_id].suspended_containers:
            if container.container_id in s.suspending:
                job = s.suspending.pop(container.container_id)
                s.queues_by_prio[job.priority].append(job)

    # ── resource stats (identical to priority) ────────────────────────────────
    pool_stats = {}
    for i in range(s.executor.num_pools):
        pool_stats[i] = {
            "avail_cpu": s.executor.pools[i].avail_cpu_pool,
            "avail_ram": s.executor.pools[i].avail_ram_pool,
            "total_cpu": s.executor.pools[i].max_cpu_pool,
            "total_ram": s.executor.pools[i].max_ram_pool,
        }

    # ── assignment loop ───────────────────────────────────────────────────────
    queues = [s.qry_jobs, s.interactive_jobs, s.batch_ppln_jobs]
    new_assignments = []

    for queue in queues:
        to_remove = []
        to_start  = []
        for job in queue:
            pool_id = get_pool_with_max_avail_ram(s, pool_stats)
            if pool_id == -1:
                break

            to_remove.append(job)
            op_list = job.ops
            rs = job.retry_stats

            # OOM retry: double previous allocation (unchanged)
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

            # Preempt-resume: restore previous allocation (unchanged)
            elif rs is not None and (rs.old_cpu < pool_stats[pool_id]["avail_cpu"] and
                                     rs.old_ram < pool_stats[pool_id]["avail_ram"]):
                job_cpu = rs.old_cpu
                job_ram = rs.old_ram

            # Newly arrived job: use estimate if available, else 10%-of-pool
            else:
                job_cpu = max(1, int(pool_stats[pool_id]["total_cpu"] / 10))
                est_ram = _estimate_ram(op_list)
                if est_ram is not None:
                    job_ram = est_ram
                    logger.debug(f"priority-est: using estimate ram={job_ram}GB for {[str(op.id)[:8] for op in op_list]}")
                else:
                    job_ram = max(1, int(pool_stats[pool_id]["total_ram"] / 10))
                    logger.debug(f"priority-est: no estimate, fallback ram={job_ram}GB")

                if job_cpu >= pool_stats[pool_id]["avail_cpu"] or job_ram >= pool_stats[pool_id]["avail_ram"]:
                    job_cpu = pool_stats[pool_id]["avail_cpu"]
                    job_ram = pool_stats[pool_id]["avail_ram"]

            asgmnt = Assignment(
                ops=op_list, cpu=job_cpu, ram=job_ram,
                priority=job.priority, pool_id=pool_id,
                pipeline_id=job.pipeline.pipeline_id if job.pipeline else "unknown_pipeline",
            )
            pool_stats[pool_id]["avail_cpu"] -= job_cpu
            pool_stats[pool_id]["avail_ram"] -= job_ram
            to_start.append(asgmnt)

        new_assignments.extend(to_start)
        for j in to_remove:
            queue.remove(j)

    # ── preemption (identical to priority) ───────────────────────────────────
    suspensions = []
    if len(s.qry_jobs) > 0:
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
            pool_id = (pool_id + 1) % s.executor.num_pools

        for sus in to_suspend:
            suspensions.append(Suspend(sus.container_id, sus.pool_id))

    return suspensions, new_assignments
