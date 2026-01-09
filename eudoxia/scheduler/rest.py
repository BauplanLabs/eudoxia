"""
REST Scheduler - delegates scheduling decisions to an external HTTP server.

This allows scheduling logic to be implemented in any language that can
serve HTTP requests. The external scheduler must implement two endpoints:

    POST /init     - Called once at startup with TOML parameters as JSON
    POST /schedule - Called each tick with current state; returns assignments/suspensions

A reference implementation in Go is provided in go/naive/main.go. Usage:

    # Terminal 1: Start the external scheduler
    cd go && go run ./naive/ -port 8080

    # Terminal 2: Run the simulation
    eudoxia run mysim.toml

Configuration (TOML):
    scheduler_algo = "rest"
    rest_scheduler_addr = "localhost:8080"      # Host:port of external scheduler
    rest_poll_interval = 1.0                    # Sim seconds between calls when idle

See README.md for the full JSON schema and more details.
"""
import logging
import time
import requests
from typing import List, Tuple, Dict

# Silence verbose urllib3 logging
logging.getLogger("urllib3").setLevel(logging.WARNING)

from .decorators import register_scheduler_init, register_scheduler
from ..executor.assignment import Assignment, ExecutionResult, Suspend
from ..workload.pipeline import Pipeline, Operator
from ..utils import Priority

logger = logging.getLogger(__name__)


@register_scheduler_init(key="rest")
def rest_init(s):
    """Initialize REST scheduler - call external /init endpoint."""
    s.rest_addr = s.params.get("rest_scheduler_addr", "localhost:8080")
    s.rest_poll_interval = s.params.get("rest_poll_interval", 1.0)
    s.last_call_sim_time = 0.0
    s.current_tick = 0

    # Track pipelines from previous ticks (some may now be complete)
    s.other_pipelines: Dict[str, Pipeline] = {}

    # Operator lookup table (uuid string -> Operator)
    s.operator_lookup: Dict[str, Operator] = {}

    # Timing stats
    s.timing_serialize = 0.0
    s.timing_http = 0.0
    s.timing_parse = 0.0
    s.timing_call_count = 0
    s.timing_wall_start = time.perf_counter()

    # Compute final tick for end-of-simulation logging
    s.final_tick = int(s.params["duration"] * s.params["ticks_per_second"])

    # Call external init
    url = f"http://{s.rest_addr}/init"
    payload = {"params": s.params}
    logger.info(f"REST scheduler: calling {url}")
    resp = requests.post(url, json=payload)
    resp.raise_for_status()  # Fail fast on error
    logger.info(f"REST scheduler: init completed successfully")


@register_scheduler(key="rest")
def rest_scheduler(s, results: List[ExecutionResult],
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Call external scheduler if conditions are met."""
    s.current_tick += 1
    ticks_per_second = s.params.get("ticks_per_second", 1000)
    current_sim_time = s.current_tick / ticks_per_second
    time_since_last = current_sim_time - s.last_call_sim_time

    # Log timing stats on final tick
    if s.current_tick == s.final_tick:
        total = time.perf_counter() - s.timing_wall_start
        logger.info(f"REST scheduler ({s.timing_call_count} calls): "
                   f"serialize={s.timing_serialize:.2f}s ({100*s.timing_serialize/total:.1f}%), "
                   f"http={s.timing_http:.2f}s ({100*s.timing_http/total:.1f}%), "
                   f"parse={s.timing_parse:.2f}s ({100*s.timing_parse/total:.1f}%)")

    # Early return if no events and poll interval not reached
    if not pipelines and not results and time_since_last < s.rest_poll_interval:
        return ([], [])

    s.last_call_sim_time = current_sim_time

    # Register new operators for response parsing
    for p in pipelines:
        for op in p.values:
            s.operator_lookup[str(op.id)] = op

    # Serialize payload
    t0 = time.perf_counter()
    payload = {
        "tick": s.current_tick,
        "sim_time_seconds": current_sim_time,
        "results": [r.to_dict() for r in results],
        "new_pipelines": [p.to_dict() for p in pipelines],
        "other_pipelines": [p.to_dict() for p in s.other_pipelines.values()],
        "pools": [pool.to_dict() for pool in s.executor.pools],
    }
    t1 = time.perf_counter()
    s.timing_serialize += t1 - t0

    # Call external scheduler
    url = f"http://{s.rest_addr}/schedule"
    resp = requests.post(url, json=payload)
    resp.raise_for_status()  # Fail fast
    t2 = time.perf_counter()
    s.timing_http += t2 - t1

    # Parse response into Assignment/Suspend objects
    response = resp.json()
    suspensions = _parse_suspensions(response["suspensions"])
    assignments = _parse_assignments(s, response["assignments"])
    t3 = time.perf_counter()
    s.timing_parse += t3 - t2

    s.timing_call_count += 1

    # Update pipeline tracking: add new, remove completed
    for p in pipelines:
        s.other_pipelines[p.pipeline_id] = p
    for pipeline_id in list(s.other_pipelines.keys()):
        pipeline = s.other_pipelines[pipeline_id]
        if pipeline.runtime_status().is_pipeline_successful():
            for op in pipeline.values:
                del s.operator_lookup[str(op.id)]
            del s.other_pipelines[pipeline_id]

    return (suspensions, assignments)


def _parse_suspensions(suspensions_json: list) -> List[Suspend]:
    """Parse suspensions from external scheduler response."""
    return [
        Suspend(container_id=s["container_id"], pool_id=s["pool_id"])
        for s in suspensions_json
    ]


def _parse_assignments(s, assignments_json: list) -> List[Assignment]:
    """Parse assignments from external scheduler response, looking up operators by UUID."""
    assignments = []
    for a in assignments_json:
        ops = [s.operator_lookup[op_id] for op_id in a["operator_ids"]]
        assignment = Assignment(
            ops=ops,
            cpu=a["cpu"],
            ram=a["ram_gb"],
            priority=Priority[a["priority"]],
            pool_id=a["pool_id"],
            pipeline_id=ops[0].pipeline.pipeline_id,
            is_resume=a["is_resume"],
            force_run=a["force_run"],
        )
        assignments.append(assignment)
    return assignments
