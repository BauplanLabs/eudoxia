# Eudoxia, a Data Pipeline Simulator

![Tests](https://github.com/BauplanLabs/eudoxia/workflows/Run%20Tests/badge.svg)

Eudoxia is a simulator for evaluating scheduling policies for data pipelines, executed in distributed environments.  The pipelines are represented as Directed Acyclic Graphs (DAGs), and node execution occurs in simulated containers.  Nodes (called "operators") in a pipeline may each be deployed to their own containers, or it is possible to batch multiple nodes together in the same container.  Either way, a node cannot execute until its parents have completed successfully.

A pipeline is considered completed when all of its operators have completed successfully.  A pipeline is never considered failed, as a scheduler may retry a failed operator.  Indeed, nodes commonly fail due to insufficient memory limits, so a scheduler may retry in a container with more memory (possibly waiting until available machines have enough memory to deploy the bigger container).  If some of the nodes in a container succeeded before a memory limit was hit, only the unfinished nodes must be retried.

Pipelines have one of three priority levels: batch (lowest), interactive, or query (highest).  A scheduler may choose to suspend running containers to free capacity for higher priority work.  Suspended work may be redeployed again later.

## Getting Started

Make sure you have Python 3.12+ installed.  If you have [venv](https://docs.python.org/3/tutorial/venv.html) available, you can install Eudoxia and its dependencies as follows:

```bash
# get the code
git clone https://github.com/BauplanLabs/eudoxia.git
cd eudoxia

# create a virtual environment
python3 -m venv venv
source venv/bin/activate

# install Eudoxia and its dependencies in the virtual environment
python3 -m pip install --editable .
```

You can test Eudoxia as follows:

```
pytest ./tests
```

Eudoxia has several main components: a workload, a scheduler, and an executor.  All these are configured by parameters in a single TOML file.  You can create a file with the default settings as follows:

```
eudoxia init mysim.toml
```

Feel free to edit mysim.toml.  The `scheduler_algo` field specifies which scheduling policy we will evaluate.  Other fields specify the hardware resources available, the workload characteristics, and other options.

You can run the simulator as follows (to produce performance statistics, such as pipeline latency):

```
eudoxia run mysim.toml
```

## Simulator Overview

Simulation proceeds in discrete **ticks**. The tick frequency is configurable via `ticks_per_second` (default: 1,000, i.e., 1 millisecond per tick). Each tick, the workload may produce new pipelines, the scheduler issues commands, and the executor advances all running containers by one tick. Higher tick frequencies provide more precise simulation at the cost of longer execution time. Log output timestamps (e.g., `[10.5s]`) show simulated time, not wall-clock time.

The simulator has three main components, each implementing a `run_one_tick` method. The **workload** generates pipelines according to configured parameters. The **scheduler** decides which operators to deploy, suspend, or retry—without knowing the true resource requirements. The **executor** manages physical resources (CPU and memory), deploys containers, advances running work, and reports completions and failures.

## Workload

The workload component delivers **pipelines** to the scheduler. Eudoxia supports two approaches: on-the-fly random generation and replay from trace files.

### Pipelines

A pipeline is a directed acyclic graph (DAG) of operators representing a user job. Each pipeline has a **priority**: `QUERY` (highest), `INTERACTIVE`, or `BATCH_PIPELINE` (lowest). Query pipelines are always single-operator; others have multiple operators.

Each operator contains one or more **segments**, which execute sequentially and represent different stages of an operator's work. Segments define resource usage independently for CPU and memory.  Eudoxia currently has limited support for multi-segment operators (built-in workload implementations do not generate/parse multi-segment operators yet).

**Memory** follows one of two patterns. If `memory_gb` is set, the segment uses that fixed amount from the start. Otherwise, memory grows linearly as data is read from storage, peaking at `storage_read_gb`. The latter pattern can trigger out-of-memory failures partway through execution if the container's memory limit is exceeded.

**CPU time** is determined by `baseline_cpu_seconds`, which specifies how long a segment takes on a single core. The `cpu_scaling` function determines how runtime decreases with more cores. For example, `const` provides no parallelism benefit.  The `linear3` option gives linear speedup up to 3 cores, then remains flat (e.g., the operator will finish in baseline_cpu_seconds/2 if it is given 2 cores, but it will only finish in baseline_cpu_seconds/3 if it is give 10 cores).  The `sqrt` option provides diminishing returns proportional to the square root of core count.

### Workload Approach 1: On-the-Fly Generation

By default, the simulator uses a `WorkloadGenerator` that creates pipelines randomly according to configured parameters. The key parameters are `waiting_seconds_mean` (average time between arrivals) and `num_pipelines` (number of pipelines per arrival). Arrival times are sampled from a normal distribution around the mean, so arrival patterns vary naturally. The `random_seed` parameter ensures reproducibility.

The priority mix is controlled by `query_prob`, `interactive_prob`, and `batch_prob`. Non-query pipelines have `num_operators` operators on average. Segments are sampled from a fixed set of prototypes with different demands for CPU and I/O resources; some do relatively more computation relative to I/O than others. The `cpu_io_ratio` parameter increases the probability that high-CPU prototypes will be used, but it does not enforce a strict ratio of CPU seconds to I/O bytes.

### Workload Approach 2: Trace Replay

As an alternative to random generation, workloads can be loaded from **trace files**. Traces are CSV files that specify exact pipelines, operators, and arrival times. They might be generated by external tools based on real workload observations, or captured from a random workload for reproducible testing.

Trace format (one row per operator):

| pipeline_id | arrival_seconds | priority    | operator_id | parents | resources... |
|-------------|-----------------|-------------|-------------|---------|--------------|
| p1          | 0.0             | INTERACTIVE | op1         |         |              |
| p1          |                 |             | op2         | op1     |              |
| p2          | 0.5             | QUERY       | op1         |         |              |

The `arrival_seconds` and `priority` columns are only set for the first operator of each pipeline. The `parents` column lists semicolon-separated operator IDs for DAG edges. Resource columns (`baseline_cpu_seconds`, `cpu_scaling`, `memory_gb`, `storage_read_gb`) define each operator's execution requirements.

Generate a trace from random workload parameters:

```
eudoxia gentrace mysim.toml workload.csv
```

Run a simulation with a trace:

```
eudoxia run mysim.toml -w workload.csv
```

Any workload generation options in mysim.toml will be ignored since are using an actual trace (workload.csv) instead of generating on-the-fly.  Be careful to make sure the simulation duration in mysim.toml is as long as the trace, unless you only want to simulate the first events of the the trace (up to the duration cutoff).

## Executor

The executor manages all physical resources and runs containers, typically representing a cluster. An executor manages one or more resource pools, each configured with `cpus_per_pool` CPUs and `ram_gb_per_pool` GB of memory. A resource pool is analogous to a machine: a container must run entirely within a single pool and cannot span multiple pools.

A **container** is an allocation of CPU and memory that executes one or more operators from the same pipeline. When a container has multiple operators, they run sequentially in DAG order; an operator cannot start until its parents have completed. The scheduler specifies the resources and operators for each container; the executor tracks memory usage tick by tick and reports completions and failures.

If memory usage exceeds the container's allocation, an out-of-memory (OOM) error occurs and the container fails. By default, the executor also enforces strict allocation limits: the sum of all container allocations in a pool cannot exceed the pool's RAM capacity. When `allow_memory_overcommit` is enabled, the scheduler may allocate more memory than physically available, betting that not all containers will reach peak usage simultaneously. If total memory consumption exceeds pool capacity, the executor's OOM killer terminates containers to bring usage back within limits. Containers are scored by `consumption * (consumption / allocation)`, and the highest-scoring container is killed first. This prioritizes killing containers that are both using significant memory and have exceeded their fair share of their allocation.

Containers can be **suspended** to free resources for higher priority work, but only between operator boundaries (not mid-operator). Suspension requires writing current state to disk, which takes time proportional to allocated RAM. Suspended operators return to pending state and can be reassigned later.

## Scheduler

The scheduler decides which operators to run, how many resources to allocate, and when to suspend work. It does not know the true resource requirements of operators; it must make decisions based on limited information and handle failures (such as OOM) by retrying with different allocations.

### Interface

Each tick, the scheduler's `run_one_tick` method receives two inputs: **results** (completions and failures from the previous tick) and **pipelines** (newly arrived work). It returns two lists: **suspensions** (containers to suspend) and **assignments** (new work to deploy).

An `Assignment` specifies a list of operators, CPU and memory allocations, priority, and target pool. A `Suspend` specifies a container ID and pool. The scheduler has access to the executor to inspect current resource availability.

### Built-in Schedulers

Four schedulers are included:

- `naive`: FIFO ordering, allocates all pool resources to one pipeline at a time, no preemption
- `priority`: serves work in priority order (query > interactive > batch), can suspend lower priority work to make room for higher priority, may starve batch work under load
- `priority-pool`: dedicates pools to priority levels (query/interactive to pool 0, batch to pool 1), requires exactly 2 pools, no preemption
- `overbook`: demonstrates memory overcommit by allocating full pool RAM to each container while only respecting CPU limits (`allow_memory_overcommit` should be enabled)

### Custom Schedulers (Python)

To implement a custom scheduler, define two functions using the registration decorators:

```python
from eudoxia.scheduler.decorators import register_scheduler_init, register_scheduler

@register_scheduler_init(key="myscheduler")
def myscheduler_init(s):
    s.my_queue = []

@register_scheduler(key="myscheduler")
def myscheduler(s, results, pipelines):
    # ... scheduling logic ...
    return suspensions, assignments
```

For convenience, you can generate starter code for a scheduler with `eudoxia init`:

```
eudoxia init mysim.toml -s myscheduler
```

This creates `myscheduler.py` with a template implementation and sets `scheduler_algo = "myscheduler"` in the TOML. Run with:

```
PYTHONPATH=. eudoxia run -i myscheduler mysim.toml
```

The `-i` option specifies a Python module to import (so that the decorators can trigger registration of the scheduler).  Unless you install your scheduler, you will generally need to specify `PYTHONPATH` as well because the import will only work on locations in the Python path (that is, `sys.path`).

Operators follow a state machine: `PENDING` → `ASSIGNED` → `RUNNING` → `COMPLETED`. Failed operators can be retried directly (`FAILED` → `ASSIGNED`). Suspended operators return to `PENDING` and can be reassigned.

An operator is **ready** to run when it can be assigned and all its parents are `COMPLETED`. The `ASSIGNABLE_STATES` constant includes both `PENDING` (new work) and `FAILED` (retries). To find ready operators in a pipeline:

```python
from eudoxia.workload.runtime_status import ASSIGNABLE_STATES

ready_ops = pipeline.runtime_status().get_ops(
    ASSIGNABLE_STATES,
    require_parents_complete=True
)
```

When `multi_operator_containers` is enabled, you may want to pass `require_parents_complete=False` to get all assignable operators, then batch them into a single container. The parents don't need to be complete if they will run first in the same container.

### External Schedulers (Not Python)

You can implement an external scheduler in any language by using the `rest` scheduler, which delegates scheduling decisions to an external HTTP server. This is useful for prototyping in other languages or integrating with external systems.

Configuration:

```toml
scheduler_algo = "rest"
rest_scheduler_addr = "localhost:8080"
rest_poll_interval = 1.0
```

The external scheduler must implement two endpoints:

- `POST /init` - Called once at startup with all TOML parameters as JSON
- `POST /schedule` - Called each tick (or at `rest_poll_interval`) with current state; returns assignments and suspensions

A reference implementation in Go is provided in `go/naive.go`. To use it:

```bash
# Terminal 1: Start the external scheduler
go run ./go/naive.go -port 8080

# Terminal 2: Run the simulation
eudoxia run mysim.toml
```

The `/schedule` endpoint receives the current simulation state:

```json
{
  "tick": 12345,
  "sim_time_seconds": 12.345,
  "results": [...],
  "new_pipelines": [...],
  "outstanding_pipelines": [...],
  "pools": [...]
}
```

And returns scheduling decisions:

```json
{
  "suspensions": [{"container_id": "c1", "pool_id": 0}],
  "assignments": [{
    "operator_ids": ["uuid1"],
    "cpu": 8,
    "ram_gb": 32,
    "pool_id": 0,
    "priority": "QUERY",
    "is_resume": false,
    "force_run": false
  }]
}
```

See `go/naive.go` for the complete JSON schema and a working example.

## Programmatic Simulation

Eudoxia can be used as a Python library for scripting experiments or integrating into other tools:

```python
from eudoxia.simulator import run_simulator

# Run with a TOML file
stats = run_simulator("mysim.toml")

# Or pass parameters directly
params = {
    "duration": 300,
    "scheduler_algo": "naive",
    "num_pools": 4,
    "cpus_per_pool": 32,
    "ram_gb_per_pool": 128,
}
stats = run_simulator(params)

print(f"Pipelines completed: {stats.pipelines_all.completion_count}")
print(f"P99 latency: {stats.pipelines_all.p99_latency_seconds:.2f}s")
```

The `run_simulator` function returns a `SimulatorStats` object containing throughput, latency percentiles, failure counts, and per-priority breakdowns.

## Configuration Parameters

All parameters can be set in the TOML file. Defaults are shown below.

**Simulation**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `duration` | 600 | Simulation length in seconds |
| `ticks_per_second` | 1000 | Tick frequency (1000 = 1ms per tick) |

**Workload Generation**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `waiting_seconds_mean` | 10.0 | Mean seconds between pipeline arrivals |
| `num_pipelines` | 4 | Pipelines per arrival |
| `num_operators` | 5 | Mean operators per pipeline |
| `cpu_io_ratio` | 0.5 | 0=IO heavy, 1=CPU heavy |
| `interactive_prob` | 0.3 | Probability of interactive priority |
| `query_prob` | 0.1 | Probability of query priority |
| `batch_prob` | 0.6 | Probability of batch priority |
| `random_seed` | 42 | Seed for reproducibility |

**Scheduler**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `scheduler_algo` | "priority" | Scheduling algorithm to use |
| `rest_scheduler_addr` | "localhost:8080" | Host:port of external scheduler (when `scheduler_algo = "rest"`) |
| `rest_poll_interval` | 1.0 | Minimum sim seconds between external scheduler calls |

**Executor**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_pools` | 8 | Number of resource pools |
| `cpus_per_pool` | 64 | CPUs per pool |
| `ram_gb_per_pool` | 256 | GB of RAM per pool |
| `multi_operator_containers` | true | Allow multiple operators per container |
| `allow_memory_overcommit` | false | Allow allocations to exceed pool RAM capacity |