import argparse
import json
import shutil
import sys
import tomllib
import numpy as np
import math
import csv
from io import StringIO
from pathlib import Path
from eudoxia.simulator import run_simulator, parse_args_with_defaults, get_param_defaults
from eudoxia.workload.csv_io import CSVWorkloadReader, CSVWorkloadWriter, WorkloadTraceGenerator
from eudoxia.workload import WorkloadGenerator
from eudoxia.tools import snap_command, jitter_command, sensitivity_command, sensitivity_sample_command, sensitivity_analysis_plot_command
import tomlkit


def run_command(params_file, workload=None):
    """Run simulation with parameters file and optional workload file"""

    # Check if params file exists
    if not Path(params_file).exists():
        print(f"Error: Parameters file '{params_file}' not found", file=sys.stderr)
        sys.exit(1)

    with open(params_file, 'rb') as f:
        params = tomllib.load(f)
    params = parse_args_with_defaults(params)

    if workload:
        # base workload on trace file
        workload_path = Path(workload)
        if not workload_path.exists():
            print(f"Error: Workload file '{workload}' not found", file=sys.stderr)
            sys.exit(1)

        with open(workload_path) as f:
            # TODO: don't assume CSV
            reader = CSVWorkloadReader(f)
            workload = reader.get_workload(params['ticks_per_second'])
            # workload is based on trace
            stats = run_simulator(params, workload=workload)
    else:
        # generate workload on the fly
        stats = run_simulator(params)

    print(f"Simulation completed:")
    print(f"  Pipelines created: {stats.pipelines_created}")
    print(f"  Containers completed: {stats.containers_completed}")
    print(f"  Container throughput: {stats.throughput:.2f} containers/sec")
    print(f"  Container P99 latency: {stats.p99_latency:.2f}s")
    print(f"  Assignments: {stats.assignments}")
    print(f"  Suspensions: {stats.suspensions}")
    print(f"  Failures: {stats.failures}")
    print(f"  Failure/error counts: {stats.failure_error_counts}")
    print()
    print("  Pipeline Stats:")
    print("  " + "-" * 68)
    print(f"  {'Priority':<15} {'Arrived':>10} {'Completed':>10} {'Mean (s)':>12} {'P99 (s)':>12}")
    print("  " + "-" * 68)
    pipeline_stats = [
        ("All", stats.pipelines_all),
        ("Query", stats.pipelines_query),
        ("Interactive", stats.pipelines_interactive),
        ("Batch", stats.pipelines_batch),
    ]
    for name, pstats in pipeline_stats:
        print(f"  {name:<15} {pstats.arrival_count:>10} {pstats.completion_count:>10} {pstats.mean_latency_seconds:>12.2f} {pstats.p99_latency_seconds:>12.2f}")
    print("  " + "-" * 68)


def gentrace_command(params_file, output_file, force=False):
    """Generate CSV workload trace from parameters file"""

    # Check if params file exists
    if not Path(params_file).exists():
        print(f"Error: Parameters file '{params_file}' not found", file=sys.stderr)
        sys.exit(1)

    # Check if output file already exists
    output_path = Path(output_file)
    if output_path.exists() and not force:
        print(f"Error: File '{output_file}' already exists. Use -f/--force to overwrite.", file=sys.stderr)
        sys.exit(1)

    # Load and parse parameters
    with open(params_file, 'rb') as f:
        params = tomllib.load(f)
    params = parse_args_with_defaults(params)

    # Randomly generated workload
    workload = WorkloadGenerator(**params)

    # Workload => Trace
    trace_generator = WorkloadTraceGenerator(
        workload=workload,
        ticks_per_second=params['ticks_per_second'],
        duration_secs=params['duration']
    )

    # Write Trace to CSV
    with open(output_file, 'w') as f:
        writer = CSVWorkloadWriter(f)

        for row in trace_generator.generate_rows():
            writer.write_row(row)

    print(f"Generated workload trace saved to {output_file}")


def mkregression_command(params_file, target_dir, force=False):
    """Create a regression test from a parameters file.

    This command:
    1. Creates the target directory
    2. Copies the params.toml file
    3. Generates a trace using gentrace
    4. Runs the simulation to capture expected output
    5. Writes expected.json with the results
    """
    params_path = Path(params_file)
    target_path = Path(target_dir)

    # Check if params file exists
    if not params_path.exists():
        print(f"Error: Parameters file '{params_file}' not found", file=sys.stderr)
        sys.exit(1)

    # Check if target directory already exists
    if target_path.exists() and not force:
        print(f"Error: Directory '{target_dir}' already exists. Use -f/--force to overwrite.", file=sys.stderr)
        sys.exit(1)

    # Create target directory
    target_path.mkdir(parents=True, exist_ok=True)

    # Copy params.toml
    target_params = target_path / 'params.toml'
    shutil.copy(params_path, target_params)
    print(f"Copied {params_file} -> {target_params}")

    # Load and parse parameters
    with open(params_file, 'rb') as f:
        params = tomllib.load(f)
    params = parse_args_with_defaults(params)

    # Generate trace
    target_trace = target_path / 'trace.csv'
    workload_gen = WorkloadGenerator(**params)
    trace_generator = WorkloadTraceGenerator(
        workload=workload_gen,
        ticks_per_second=params['ticks_per_second'],
        duration_secs=params['duration']
    )
    with open(target_trace, 'w') as f:
        writer = CSVWorkloadWriter(f)
        for row in trace_generator.generate_rows():
            writer.write_row(row)
    print(f"Generated trace -> {target_trace}")

    # Run simulation with the trace to get expected output
    with open(target_trace) as f:
        reader = CSVWorkloadReader(f)
        workload_trace = reader.get_workload(params['ticks_per_second'])
        stats = run_simulator(params, workload=workload_trace)

    # Write expected.json
    target_expected = target_path / 'expected.json'
    with open(target_expected, 'w') as f:
        json.dump(stats.to_dict(), f, indent=2)
    print(f"Wrote expected output -> {target_expected}")

    print(f"\nRegression test created in {target_dir}/")
    print(f"  params.toml  - scheduler configuration")
    print(f"  trace.csv    - workload trace ({stats.pipelines_created} pipelines)")
    print(f"  expected.json - expected results ({stats.containers_completed} containers completed, {stats.suspensions} suspensions)")


SCHEDULER_TEMPLATE = '''\
from typing import List, Tuple
from eudoxia.workload import Pipeline, OperatorState
from eudoxia.executor.assignment import Assignment, ExecutionResult, Suspend
from eudoxia.scheduler.decorators import register_scheduler_init, register_scheduler


@register_scheduler_init(key="{scheduler_name}")
def {scheduler_name}_init(s):
    """Initialize scheduler state.

    Args:
        s: The scheduler instance. You can add custom attributes here.
    """
    s.waiting_queue: List[Pipeline] = []


@register_scheduler(key="{scheduler_name}")
def {scheduler_name}_scheduler(s, results: List[ExecutionResult],
                   pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    A simple FIFO scheduler that assigns one operator at a time per pool.

    Args:
        s: The scheduler instance (with access to s.executor, s.waiting_queue, etc.)
        results: List of execution results from previous tick
        pipelines: List of new pipelines from WorkloadGenerator

    Returns:
        Tuple[List[Suspend], List[Assignment]]:
            - List of containers to suspend
            - List of new assignments to make
    """
    for p in pipelines:
        s.waiting_queue.append(p)

    suspensions = []
    assignments = []
    requeue_pipelines = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        while s.waiting_queue:
            pipeline = s.waiting_queue.pop(0)
            status = pipeline.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if status.is_pipeline_successful() or has_failures:
                # don't retry failures; drop completed/failed pipelines
                continue
            requeue_pipelines.append(pipeline)

            op_list = status.get_assignable_ops()[:1]
            if not op_list:
                continue

            assignment = Assignment(
                ops=op_list,
                cpu=avail_cpu,
                ram=avail_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id
            )
            assignments.append(assignment)
            break

    s.waiting_queue.extend(requeue_pipelines)
    return suspensions, assignments
'''


def init_command(output_file, force=False, scheduler_name=None):
    """Create TOML file with default parameters, and optionally a starter scheduler."""
    output_path = Path(output_file)

    # Check if file already exists
    if output_path.exists() and not force:
        print(f"Error: File '{output_file}' already exists. Use --force to overwrite.", file=sys.stderr)
        sys.exit(1)

    # Get default parameters
    defaults = get_param_defaults()

    # If scheduler name provided, create the scheduler file and update config
    if scheduler_name:
        scheduler_file = f"{scheduler_name}.py"
        scheduler_path = output_path.parent / scheduler_file

        if scheduler_path.exists() and not force:
            print(f"Error: Scheduler file '{scheduler_path}' already exists. Use --force to overwrite.", file=sys.stderr)
            sys.exit(1)

        # Write scheduler file
        with open(scheduler_path, 'w') as f:
            f.write(SCHEDULER_TEMPLATE.format(scheduler_name=scheduler_name))

        print(f"Created scheduler file: {scheduler_path}")

        # Update defaults to use the new scheduler
        defaults['scheduler_algo'] = scheduler_name

    # Create TOML table and update with defaults
    t = tomlkit.table()
    t.update(defaults)

    # Write to file
    with open(output_path, 'w') as f:
        tomlkit.dump(t, f)

    print(f"Created default parameters file: {output_file}")

    if scheduler_name:
        print(f"\nTo run with your custom scheduler:")
        print(f"  PYTHONPATH=. eudoxia run -i {scheduler_name} {output_file}")


# entry point if the user just runs "eudoxia".  In that case, argv
# will be None, but parser.parse_args will check sys.argv.
#
# other scripts can optionally call main directly, with desired args.
def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='eudoxia',
        description='Run Eudoxia simulation'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run subcommand
    run_parser = subparsers.add_parser('run', help='Run simulation')
    run_parser.add_argument('params_file', help='Path to TOML parameters file')
    run_parser.add_argument('-w', '--workload', help='Path to CSV workload file')
    run_parser.add_argument('-i', '--import', dest='imports', action='append', default=[],
                           metavar='MODULE', help='Import a module (can be repeated). '
                           'Useful for loading custom schedulers with @register_scheduler decorators.')
    
    # Gentrace subcommand
    gentrace_parser = subparsers.add_parser('gentrace', help='Generate CSV workload trace from parameters')
    gentrace_parser.add_argument('params_file', help='Path to TOML parameters file')
    gentrace_parser.add_argument('output_file', help='Path to output CSV workload file')
    gentrace_parser.add_argument('-f', '--force', action='store_true', help='Overwrite existing file')
    
    # Init subcommand
    init_parser = subparsers.add_parser('init', help='Create TOML file with default parameters')
    init_parser.add_argument('output_file', help='Path to output TOML parameters file')
    init_parser.add_argument('-s', '--scheduler', metavar='NAME',
                            help='Create a starter scheduler file named NAME.py')
    init_parser.add_argument('-f', '--force', action='store_true', help='Overwrite existing files')

    # mkregression subcommand
    regression_parser = subparsers.add_parser('mkregression',
                                              help='Create a regression test from a parameters file')
    regression_parser.add_argument('params_file', help='Path to TOML parameters file')
    regression_parser.add_argument('target_dir', help='Directory to create the regression test in')
    regression_parser.add_argument('-f', '--force', action='store_true',
                                   help='Overwrite existing directory')

    # Tools command group
    tools_parser = subparsers.add_parser('tools', help='Workload manipulation and analysis tools')
    tools_subparsers = tools_parser.add_subparsers(dest='tool_command', help='Available tools')

    # Snap subcommand under tools
    snap_parser = tools_subparsers.add_parser('snap', help='Snap timestamps to tick boundaries (round down)')
    snap_parser.add_argument('input_workload', help='Path to input CSV workload file')
    snap_parser.add_argument('output_file', help='Path to output CSV workload file')
    snap_parser.add_argument('ticks_per_second', type=int, help='Number of ticks per second')
    snap_parser.add_argument('-f', '--force', action='store_true', help='Overwrite existing file')

    # Jitter subcommand under tools
    jitter_parser = tools_subparsers.add_parser('jitter', help='Add random jitter to timestamps')
    jitter_parser.add_argument('input_workload', help='Path to input CSV workload file')
    jitter_parser.add_argument('output_file', help='Path to output CSV workload file')
    jitter_parser.add_argument('delta', type=float, help='Maximum jitter to add (uniform distribution [0, delta])')
    jitter_parser.add_argument('-s', '--seed', type=int, help='Random seed (default: 42)')
    jitter_parser.add_argument('-f', '--force', action='store_true', help='Overwrite existing file')

    # Sensitivity subcommand under tools
    sensitivity_parser = tools_subparsers.add_parser('sensitivity', help='Run sensitivity analysis with workload mutations')
    sensitivity_parser.add_argument('params_file', help='Path to TOML parameters file')
    sensitivity_parser.add_argument('workload', help='Path to CSV workload file')
    sensitivity_parser.add_argument('output_dir', help='Directory to save results')
    sensitivity_parser.add_argument('--jitter-seed', type=int, help='Random seed for jitter mutations (default: 42)')

    # Sensitivity-sample subcommand under tools
    sensitivity_sample_parser = tools_subparsers.add_parser('sensitivity-sample', help='Run sensitivity analysis on multiple generated workloads')
    sensitivity_sample_parser.add_argument('params_file', help='Path to TOML parameters file')
    sensitivity_sample_parser.add_argument('output_dir', help='Directory to save results')
    sensitivity_sample_parser.add_argument('sample_size', type=int, help='Number of workload samples to generate')
    sensitivity_sample_parser.add_argument('--start-seed', type=int, default=42, help='Starting seed for workload generation (default: 42)')
    sensitivity_sample_parser.add_argument('--jitter-seed', type=int, help='Random seed for jitter mutations (default: 42)')

    # Sensitivity-analysis-plot subcommand under tools
    sensitivity_plot_parser = tools_subparsers.add_parser('sensitivity-analysis-plot', help='Generate plots from sensitivity analysis results')
    sensitivity_plot_parser.add_argument('results_dir', help='Directory containing results.csv from sensitivity-sample command')

    args = parser.parse_args(argv)
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == 'run':
        # Import user-specified modules (e.g., custom schedulers)
        for module_name in args.imports:
            try:
                __import__(module_name)
            except ImportError as e:
                print(f"Error: Could not import module '{module_name}': {e}", file=sys.stderr)
                print(f"sys.path:", file=sys.stderr)
                for p in sys.path:
                    print(f" - {p}", file=sys.stderr)
                print(f"\nTo add the current directory to the path:", file=sys.stderr)
                print(f"  PYTHONPATH=. eudoxia run ...", file=sys.stderr)
                sys.exit(1)
        run_command(args.params_file, workload=args.workload)
    elif args.command == 'gentrace':
        gentrace_command(args.params_file, args.output_file, force=args.force)
    elif args.command == 'init':
        init_command(args.output_file, force=args.force, scheduler_name=args.scheduler)
    elif args.command == 'mkregression':
        mkregression_command(args.params_file, args.target_dir, force=args.force)
    elif args.command == 'tools':
        if args.tool_command == 'snap':
            snap_command(args.input_workload, args.output_file, args.ticks_per_second, force=args.force)
        elif args.tool_command == 'jitter':
            jitter_command(args.input_workload, args.output_file, args.delta, seed=args.seed, force=args.force)
        elif args.tool_command == 'sensitivity':
            sensitivity_command(args.params_file, args.workload, args.output_dir, jitter_seed=args.jitter_seed)
        elif args.tool_command == 'sensitivity-sample':
            sensitivity_sample_command(args.params_file, args.output_dir, args.sample_size,
                                      start_seed=args.start_seed, jitter_seed=args.jitter_seed)
        elif args.tool_command == 'sensitivity-analysis-plot':
            sensitivity_analysis_plot_command(args.results_dir)
        else:
            tools_parser.print_help()
            sys.exit(1)
    else:
        print(f"Error: Unknown command '{args.command}'. Available commands: run, gentrace, init, tools", file=sys.stderr)
        sys.exit(1)


# entry point if the user runs "python3 -m eudoxia"
if __name__ == '__main__':
    main()
