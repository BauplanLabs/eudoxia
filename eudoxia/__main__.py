import argparse
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
    print(f"  Pipelines completed: {stats.pipelines_completed}")
    print(f"  Throughput: {stats.throughput:.2f} pipelines/sec")
    print(f"  P99 latency: {stats.p99_latency:.2f}s")
    print(f"  Assignments: {stats.assignments}")
    print(f"  Suspensions: {stats.suspensions}")
    print(f"  Failures: {stats.failures}")
    print(f"  Failure/error counts: {stats.failure_error_counts}")


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


def init_command(output_file, force=False):
    """Create TOML file with default parameters"""
    output_path = Path(output_file)

    # Check if file already exists
    if output_path.exists() and not force:
        print(f"Error: File '{output_file}' already exists. Use --force to overwrite.", file=sys.stderr)
        sys.exit(1)

    # Get default parameters
    defaults = get_param_defaults()

    # Create TOML table and update with defaults
    t = tomlkit.table()
    t.update(defaults)

    # Write to file
    with open(output_path, 'w') as f:
        tomlkit.dump(t, f)

    print(f"Created default parameters file: {output_file}")


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
    
    # Gentrace subcommand
    gentrace_parser = subparsers.add_parser('gentrace', help='Generate CSV workload trace from parameters')
    gentrace_parser.add_argument('params_file', help='Path to TOML parameters file')
    gentrace_parser.add_argument('output_file', help='Path to output CSV workload file')
    gentrace_parser.add_argument('-f', '--force', action='store_true', help='Overwrite existing file')
    
    # Init subcommand
    init_parser = subparsers.add_parser('init', help='Create TOML file with default parameters')
    init_parser.add_argument('output_file', help='Path to output TOML parameters file')
    init_parser.add_argument('-f', '--force', action='store_true', help='Overwrite existing file')

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
        run_command(args.params_file, workload=args.workload)
    elif args.command == 'gentrace':
        gentrace_command(args.params_file, args.output_file, force=args.force)
    elif args.command == 'init':
        init_command(args.output_file, force=args.force)
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
