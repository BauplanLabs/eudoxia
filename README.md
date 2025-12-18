# Eudoxia 

![Tests](https://github.com/BauplanLabs/eudoxia/workflows/Run%20Tests/badge.svg)

Eudoxia is a simulator for evaluating scheduling policies for data pipelines, executed in distributed environments.  The pipelines are represented as directed-acyclic graphs (DAGs), and node execution occurs in simulated containers.  Nodes (called "operators") in a pipeline may each be deployed to their own containers, or it is possible to batch multiple nodes together in the same container.  Either way, a node cannot execute until its parents have completed successfully.

A pipeline is considered completed when all of its operators have completed successfully.  A pipeline is never considered failed, as a scheduler may retry a failed operator.  Indeed, nodes commonly fail due to insufficient memory limits, so a scheduler may retry in a container with more memory (possibly waiting until available machines have enough memory to deploy the bigger container).  If some of the nodes in a container succeeded before a memory limit was hit, only the unfinished nodes must be retried.

Pipelines have one of three priority levels: batch (lowest), interactive, or query (highest).  A scheduler may choose to suspend running containers, to free capacity for higher priority work.  Suspended work may be redeployed again later.

## Getting Started

Make sure you have Python 3.12+ installed.  If you have (venv)[https://docs.python.org/3/tutorial/venv.html] available, you can install Eudoxia and its dependencies as follows:

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

You can run the simulator as follows, to produce performance statistics (such as pipeline latency):

```
eudoxia run mysim.toml
```

## Simulator Overview

 - components: workload, executor, scheduler
 - ticks

## Workload

 - pipelines, operators, segments
 - CPU and memory for segment
 - random generation, seeds
 - traces: how to generate, and exceution.  Limitation: one segment per op.

## Executor

 - executor: all resources
 - resource pool: like a machine, cannot split job across these
 - containers, and what we can do with them

## Scheduler

### Interface

### Built-in Schedulers

 - priority
 - priortiy pool
 - naive

### Custom Schedulers

 - how to initialize one from the cmdline
 - how to use it

## Programmatic Simulation

 - brief example

## Configuration Parameters

 - can we base it off the comments?