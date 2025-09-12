# Eudoxia API Documentation 
This file describes the public API for `eudoxia` and the config parameters that can be placed in a `params.toml` file to dictate the scenario simulated. 


## API
There is one function at the moment that should be directly accessed:

### run_simulator(param_file: str)
The main method to run the simulator. There are three core entities, WorkloadGenerator, Scheduler, and Executor. Each offers a `run_one_tick` function. This function encodes the core event loop which runs one tick per iteration and handles that by running each of the three in sequence, passing the results of one function to another
#### Args: 
- `param_file`: `str`
    - Path (relative or absolute) to paramater file. Must be a TOML file. 


    run_simulator(param_file: str)

#### Raises
- `FileNotFoundError`: If param file does not exist
- `TOMLDecodeError`: If the file is not in proper TOML format 

## Paramaters
- `ticks_per_second`: number of simulation ticks per second (controls simulation granularity)
    - Default: `100,000` (equivalent to 10 microseconds per tick)
- `waiting_seconds_mean`: how many seconds on average the dispatcher will wait between generating pipelines
    - Default: `10.0`
- `num_piplines`: mean number of pipelines to generate when pipelines created
    - Default: `4`
- `num_operators`: mean number of operators per pipelines
    - Default: `5`
- `num_segs`: **NOT IN USE all operators have single segment** number of segments per operator
    - Default: `1`
- The following three dictate probabilities for different query types (interactive, query, batch). They ***must*** sum to 1
    - `interactive_prob`
        - Default value `0.3`
    - `query_prob`
        - Default value `0.1`
    - `batch_prob`
        - Default value `0.6`
- `cpu_io_ratio`: Value between 0 and 1 indicating on average how IO heavy a segment is. Low value is IO heavy. 
    - Default: `0.5`
