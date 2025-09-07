import argparse
import sys
import tomllib
from pathlib import Path
from eudoxia.simulator import run_simulator, parse_args_with_defaults
from eudoxia.workload.csv_io import CSVWorkloadReader


def run_command(args):
    """Handle the run subcommand"""
    workload = None
    if args.workload:
        workload_path = Path(args.workload)
        if not workload_path.exists():
            print(f"Error: Workload file '{args.workload}' not found", file=sys.stderr)
            sys.exit(1)
        
        # Load params to get tick_length_secs
        with open(args.params_file, 'rb') as param_file:
            params = tomllib.load(param_file)
        params = parse_args_with_defaults(params)
        
        # Create workload from CSV
        with open(workload_path, 'r') as f:
            # TODO: don't assume it is a CSV file
            reader = CSVWorkloadReader(f)
            workload = reader.get_workload(params['tick_length_secs'])
    
    # Run the simulation
    stats = run_simulator(args.params_file, workload=workload)
    
    print(f"Simulation completed:")
    print(f"  Pipelines created: {stats.pipelines_created}")
    print(f"  Pipelines completed: {stats.pipelines_completed}")
    print(f"  OOM failures: {stats.oom_failures}")
    print(f"  Throughput: {stats.throughput:.2f} pipelines/sec")
    print(f"  P99 latency: {stats.p99_latency:.2f}s")


def main(argv):
    parser = argparse.ArgumentParser(
        prog='python3 -m eudoxia',
        description='Run Eudoxia simulation'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run subcommand
    run_parser = subparsers.add_parser('run', help='Run simulation')
    run_parser.add_argument('params_file', help='Path to TOML parameters file')
    run_parser.add_argument('-w', '--workload', help='Path to CSV workload file')
    
    args = parser.parse_args(argv)
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    # Check if params file exists
    if not Path(args.params_file).exists():
        print(f"Error: Parameters file '{args.params_file}' not found", file=sys.stderr)
        sys.exit(1)
    
    if args.command == 'run':
        run_command(args)
    else:
        print(f"Error: Unknown command '{args.command}'. Available commands: run", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])
