import argparse
import sys
import tomllib
import numpy as np
from io import StringIO
from pathlib import Path
from eudoxia.simulator import run_simulator, parse_args_with_defaults, get_param_defaults
from eudoxia.workload.csv_io import CSVWorkloadReader, CSVWorkloadWriter, WorkloadTraceGenerator
from eudoxia.workload import WorkloadGenerator
import tomlkit


def run_command(args):
    """Handle the run subcommand"""
    
    # Check if params file exists
    if not Path(args.params_file).exists():
        print(f"Error: Parameters file '{args.params_file}' not found", file=sys.stderr)
        sys.exit(1)

    with open(args.params_file, 'rb') as f:
        params = tomllib.load(f)
    params = parse_args_with_defaults(params)

    if args.workload:
        # base workload on trace file
        workload_path = Path(args.workload)
        if not workload_path.exists():
            print(f"Error: Workload file '{args.workload}' not found", file=sys.stderr)
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


def gentrace_command(args):
    """Handle the gentrace subcommand"""
    
    # Check if params file exists
    if not Path(args.params_file).exists():
        print(f"Error: Parameters file '{args.params_file}' not found", file=sys.stderr)
        sys.exit(1)
    
    # Check if output file already exists
    output_path = Path(args.output_file)
    if output_path.exists() and not args.force:
        print(f"Error: File '{args.output_file}' already exists. Use -f/--force to overwrite.", file=sys.stderr)
        sys.exit(1)
    
    # Load and parse parameters
    with open(args.params_file, 'rb') as f:
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
    with open(args.output_file, 'w') as f:
        writer = CSVWorkloadWriter(f)

        for row in trace_generator.generate_rows():
            writer.write_row(row)

    print(f"Generated workload trace saved to {args.output_file}")


def init_command(args):
    """Handle the init subcommand"""
    output_path = Path(args.output_file)
    
    # Check if file already exists
    if output_path.exists() and not args.force:
        print(f"Error: File '{args.output_file}' already exists. Use --force to overwrite.", file=sys.stderr)
        sys.exit(1)
    
    # Get default parameters
    defaults = get_param_defaults()
    
    # Create TOML table and update with defaults
    t = tomlkit.table()
    t.update(defaults)
    
    # Write to file
    with open(output_path, 'w') as f:
        tomlkit.dump(t, f)
    
    print(f"Created default parameters file: {args.output_file}")


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
    
    args = parser.parse_args(argv)
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'run':
        run_command(args)
    elif args.command == 'gentrace':
        gentrace_command(args)
    elif args.command == 'init':
        init_command(args)
    else:
        print(f"Error: Unknown command '{args.command}'. Available commands: run, gentrace, init", file=sys.stderr)
        sys.exit(1)


# entry point if the user runs "python3 -m eudoxia"
if __name__ == '__main__':
    main()
