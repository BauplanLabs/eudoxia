"""Tools for workload manipulation and sensitivity analysis"""

import sys
import csv
import math
import tomllib
import numpy as np
from pathlib import Path
from eudoxia.simulator import run_simulator, parse_args_with_defaults
from eudoxia.workload.csv_io import CSVWorkloadReader, CSVWorkloadWriter, WorkloadTraceGenerator
from eudoxia.workload import WorkloadGenerator


def snap_command(input_workload, output_file, ticks_per_second, force=False):
    """Snap timestamps to tick boundaries (round down)"""

    # Check if input file exists
    input_path = Path(input_workload)
    if not input_path.exists():
        print(f"Error: Input workload file '{input_workload}' not found", file=sys.stderr)
        sys.exit(1)

    # Check if output file already exists
    output_path = Path(output_file)
    if output_path.exists() and not force:
        print(f"Error: File '{output_file}' already exists. Use -f/--force to overwrite.", file=sys.stderr)
        sys.exit(1)

    # Ensure input and output are different files
    if input_path.resolve() == output_path.resolve():
        print(f"Error: Input and output files must be different", file=sys.stderr)
        sys.exit(1)

    # Validate ticks_per_second
    if ticks_per_second <= 0:
        print(f"Error: ticks_per_second must be positive, got {ticks_per_second}", file=sys.stderr)
        sys.exit(1)

    # Process the CSV
    with open(input_path) as infile, open(output_path, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            # Modify arrival_seconds if it's set (not empty)
            if row['arrival_seconds'].strip():
                original = float(row['arrival_seconds'])
                snapped = math.floor(original * ticks_per_second) / ticks_per_second
                row['arrival_seconds'] = snapped

            writer.writerow(row)

    print(f"Snapped workload timestamps saved to {output_file}")


def jitter_command(input_workload, output_file, delta, seed=None, force=False):
    """Add random jitter to timestamps"""

    # Check if input file exists
    input_path = Path(input_workload)
    if not input_path.exists():
        print(f"Error: Input workload file '{input_workload}' not found", file=sys.stderr)
        sys.exit(1)

    # Check if output file already exists
    output_path = Path(output_file)
    if output_path.exists() and not force:
        print(f"Error: File '{output_file}' already exists. Use -f/--force to overwrite.", file=sys.stderr)
        sys.exit(1)

    # Ensure input and output are different files
    if input_path.resolve() == output_path.resolve():
        print(f"Error: Input and output files must be different", file=sys.stderr)
        sys.exit(1)

    # Validate delta
    if delta < 0:
        print(f"Error: delta must be non-negative, got {delta}", file=sys.stderr)
        sys.exit(1)

    # Set random seed (use 42 as default if not provided)
    seed = seed if seed is not None else 42
    rng = np.random.default_rng(seed)

    # Read all rows and group into pipelines
    with open(input_path) as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames

        pipelines = []  # List of (arrival_seconds, [rows])
        current_pipeline_rows = []
        current_pipeline_id = None
        current_arrival = None

        for row in reader:
            pipeline_id = row['pipeline_id']

            if pipeline_id != current_pipeline_id:
                # New pipeline - save previous if exists
                if current_pipeline_rows:
                    pipelines.append((current_arrival, current_pipeline_rows))

                # Start new pipeline
                current_pipeline_id = pipeline_id

                # First row must have arrival_seconds
                if not row['arrival_seconds'].strip():
                    print(f"Error: First row of pipeline {pipeline_id} missing arrival_seconds", file=sys.stderr)
                    sys.exit(1)

                # Add jitter in range [0, delta]
                original = float(row['arrival_seconds'])
                jitter = rng.uniform(0, delta)
                jittered = original + jitter
                row['arrival_seconds'] = jittered
                current_arrival = jittered
                current_pipeline_rows = [row]
            else:
                # Same pipeline - verify no arrival_seconds
                if row['arrival_seconds'].strip():
                    print(f"Error: Non-first row of pipeline {pipeline_id} has arrival_seconds", file=sys.stderr)
                    sys.exit(1)

                current_pipeline_rows.append(row)

        # Don't forget the last pipeline
        if current_pipeline_rows:
            pipelines.append((current_arrival, current_pipeline_rows))

    # Sort pipelines by arrival_seconds
    pipelines.sort(key=lambda x: x[0])

    # Write sorted pipelines
    with open(output_path, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for arrival, rows in pipelines:
            for row in rows:
                writer.writerow(row)

    print(f"Jittered workload timestamps saved to {output_file}")


def sensitivity_analysis(params_file, workload, output_dir, jitter_seed=None):
    """
    Run sensitivity analysis with snap, jitter, and tick mutations.

    Args:
        params_file: Path to TOML parameters file
        workload: Path to CSV workload file
        output_dir: Directory to save results and intermediate files
        jitter_seed: Random seed for jitter mutations (default: 42)
    """

    # Check if params file exists
    if not Path(params_file).exists():
        print(f"Error: Parameters file '{params_file}' not found", file=sys.stderr)
        sys.exit(1)

    # Verify workload file exists
    if not Path(workload).exists():
        print(f"Error: Workload file '{workload}' not found", file=sys.stderr)
        sys.exit(1)

    # Create output directory if it doesn't exist
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    # Create workloads subdirectory
    workloads_dir = output_dir_path / "workloads"
    workloads_dir.mkdir(exist_ok=True)

    # Load parameters
    with open(params_file, 'rb') as f:
        params = tomllib.load(f)
    params = parse_args_with_defaults(params)

    # Generate ticks_per_second values: powers of 10 <= config value
    config_tps = params['ticks_per_second']
    ticks_per_second_values = []
    power = 1
    while power <= config_tps:
        ticks_per_second_values.append(power)
        power *= 10

    print(f"Testing ticks_per_second values: {ticks_per_second_values}")
    print(f"(powers of 10 ≤ {config_tps} from config)\n")

    # Prepare results collection
    results = []

    # Set random seed for jitter (use 42 as default if not provided)
    jitter_seed = jitter_seed if jitter_seed is not None else 42

    # Run sensitivity analysis for each ticks_per_second value
    for tps in ticks_per_second_values:
        print(f"\n{'='*60}")
        print(f"Running sensitivity analysis for ticks_per_second={tps}")
        print(f"{'='*60}")

        # 1. SNAP mutation
        print(f"\n[1/3] Running SNAP mutation (tps={tps})...")
        snap_workload = workloads_dir / f"snap_{tps}.csv"

        # Use snap_command to create snapped workload
        snap_command(workload, str(snap_workload), tps, force=True)

        # Run simulation with snapped workload
        params_snap = params.copy()
        params_snap['ticks_per_second'] = tps
        with open(snap_workload) as f:
            reader = CSVWorkloadReader(f)
            snap_wl = reader.get_workload(tps)
            snap_stats = run_simulator(params_snap, workload=snap_wl)

        results.append({
            'mutation_type': 'snap',
            'ticks_per_second': tps,
            **snap_stats._asdict()
        })
        print(f"  Throughput: {snap_stats.throughput:.2f}, P99 latency: {snap_stats.p99_latency:.2f}s")

        # 2. JITTER mutation
        print(f"\n[2/3] Running JITTER mutation (tps={tps}, delta={1/tps:.6f})...")
        jitter_workload = workloads_dir / f"jitter_{tps}.csv"
        delta = 1 / tps

        # Use jitter_command to create jittered workload
        jitter_command(workload, str(jitter_workload), delta, seed=jitter_seed, force=True)

        # Run simulation with jittered workload
        params_jitter = params.copy()
        params_jitter['ticks_per_second'] = tps
        with open(jitter_workload) as f:
            reader = CSVWorkloadReader(f)
            jitter_wl = reader.get_workload(tps)
            jitter_stats = run_simulator(params_jitter, workload=jitter_wl)

        results.append({
            'mutation_type': 'jitter',
            'ticks_per_second': tps,
            **jitter_stats._asdict()
        })
        print(f"  Throughput: {jitter_stats.throughput:.2f}, P99 latency: {jitter_stats.p99_latency:.2f}s")

        # 3. TICK mutation (use original workload, just change ticks_per_second)
        print(f"\n[3/3] Running TICK mutation (tps={tps})...")

        # Run simulation with modified ticks_per_second
        params_tick = params.copy()
        params_tick['ticks_per_second'] = tps
        with open(workload) as f:
            reader = CSVWorkloadReader(f)
            tick_wl = reader.get_workload(tps)
            tick_stats = run_simulator(params_tick, workload=tick_wl)

        results.append({
            'mutation_type': 'tick',
            'ticks_per_second': tps,
            **tick_stats._asdict()
        })
        print(f"  Throughput: {tick_stats.throughput:.2f}, P99 latency: {tick_stats.p99_latency:.2f}s")

    # Write results to CSV
    output_csv = output_dir_path / "results.csv"
    with open(output_csv, 'w', newline='') as f:
        fieldnames = ['mutation_type', 'ticks_per_second', 'pipelines_created', 'pipelines_completed',
                     'throughput', 'p99_latency', 'assignments', 'suspensions', 'failures', 'failure_error_counts']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n{'='*60}")
    print(f"Sensitivity analysis complete!")
    print(f"Results saved to: {output_csv}")
    print(f"Mutated workloads saved to: {workloads_dir}/")
    print(f"{'='*60}")
