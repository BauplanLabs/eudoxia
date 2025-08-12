# Eudoxia 

![Tests](https://github.com/BauplanLabs/eudoxia/workflows/Run%20Tests/badge.svg)

Eudoxia provides a simulator for a cloud execution environment. The goal is to evaluate the performance impact of different scheduling policies and algorithm in the cloud without requiring full overhead of implementation or the exorbitant cost of running workloads in the cloud. 

Eudoxia is built to be used as a simple python package. All that is required is writing a `params.toml` file, and starting the simulator is as simple as running:
```python
eudoxia.run_simulator("params.toml")
```

## Getting Started 

This project requires Python 3.12 or higher, which works much easier with a virtual environment. You can create one in a number of ways, but a simple one is to use the default `venv` package that ships with the standard python distribution. 

`python3 -m venv venv_folder` which will create `venv_folder` in the current directory (you can specify a full path). You can activate the virtual environment via `source venv_folder/bin/activate`. 

Then you want to install `eudoxia` via `python3 -m pip install .` If you wish to develop with it, install it in editable mode `python3 -m pip install --editable .`

Finally, you can make sure the install worked by running one of the pre-made configuration tests in the `tests` folder by running `python3 test.py priority-pool/params.toml`

```python
# install eudoxia in editable mode so updates to source code do not require re-installing
python3 -m venv venv
source venv/bin/activate

# when this is fully deployed: python3 -m pip install eudoxia
python3 -m pip install --editable .

# run tests 
cd tests
python3 test.py priority-pool/params.toml
```

See `Eudoxia Paramaters.md` for full configuration/API descriptions. 

# Terminology
## Submitted Queries/Jobs
1. Pipeline: 
    - The top-level abstraction. All user-submitted jobs are called Pipelines. Pipelines can be thought of as a DAG (directed, acyclic graph) of a sequence of function calls for instance.
2. Operator
    - Each node of a pipeline is called an Operator. This logically represents single function executing within a pipeline 
3. Segment
    - Each Operator is comprised of a single Segment. A Segment represents the CPU and IO resources consumed while a function runs. So if a function consumes at its peak 100MB of RAM and takes 10 seconds on 1CPU, that would be encoded in the Segment 
    - Each Segment contains the ground truth information on how much RAM this Operator will use and how the runtime will scale with the number of CPUs provided (i.e. how much speedup is achievable via parallelism). 
 
### Type
Pipelines are also annotated with the context in which they were submitted. This is represented as an enum. 
1. Query: 
    - A single-operator Pipeline (i.e. simple SQL query) which is submitted interactively by an analyst. This is given the highest priority as users expect a particularly quick turnaround time on these types of Pipelines
2. Interactive: 
    - Any pipeline submitted interactively 
3. Batch:
    - Pipelines submitted as part of an batch and not necessarily on a critical path for an analyst or user. These tend to have slightly looser latency constraints.

### Example
For example, consider an ML pipeline which first executes a SQL query on underlying data in a Parquet file to extract data in a certain format, that data is then passed to a python function which performs some normalization steps and loads it into a numpy array, and finally that array is passed to a python function which trains a simple regression model on the data prepared. This entire job would be represented as a single `Pipeline` comprised of 3 `Operators`, one for the SQL, one for the preparation/normalization, one for the training, and each operator is comprised of a single `Segment` which represents the actual IO and CPU resources consumed when the function runs. 

## Ticks 
Each iteration of the simulator is logically defined as a `Tick` for which the default duration is approximated at that of a CPU tick though this can be edited in `utils/consts.py`.  

## Modules
There are three modules each of which implement a `run_one_tick` function.
### Dispatcher
This class handles generating workloads according to the parameters set by `Params.toml`. This `run_one_tick` function takes no arguments and outputs a list of pipelines that were generated. In ticks where no pipelines are generated, this is an empty list. 

### Scheduler
This class is where scheduler implementations are loaded. This is paramaterized by the `scheduling_algo` config value in `params.toml`. This `run_one_tick` function takes in a list of pipelines that have failed during execution during the last iteration and the newly generated pipelines. It outputs a list of commands: what Pipelines to suspend in execution, and what Pipelines to allocate resources for an execute. The Scheduler does not see the Segment's ground truth RAM or CPU scaling information. Rather, it can see a limited amount of information (such as the number of operators and query type) and makes scheduling and allocation decisions based on this. 

Different implementation can be written and registered with the codebase to execute. 

The Scheduler has access to the Executor object so it can see the current state of what resources are allocated and what resources are available. 


### Executor
This is the class which manages all physical resources such as RAM and CPUs. When instructed to by the Scheduler, the Executor allocates a `Container` which is an object with a set amount of CPUs and RAM and a set of Operators to execute within it (an entire pipeline doesn't need to execute in a single `Container`). 

The `Container` calculates, using `Segment` properties, the number of ticks it will take to complete.

Each call of `run_one_tick` the Executor first suspends all jobs that it is instructed to via the `suspensions` argument and frees those resources, allocates resources according to the `assignments` argument, and then iterates over all running `Containers` and decrements the number of ticks each has left to run. For any that complete, it updates its performance statistics and frees those resources. 

If a `Container` is allocated with insufficient RAM for the Pipeline running (recall that the Scheduler does not have access to the Segment information when making scheduling/allocation decisions), it will run for as many ticks as it takes to load more data than RAM allocated before failing. When a failure occurs, the resources are freed and the failure is returned for the scheduler to consider. 