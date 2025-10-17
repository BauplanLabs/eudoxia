import io
import tempfile
import unittest
import csv
from unittest.mock import patch
from eudoxia.__main__ import main


class TestCommandLine(unittest.TestCase):
    
    def test_run_with_params_only(self):
        """Test run command with just params file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write('duration = 0.001\n')  # Very short duration for fast test
            params_file = f.name
        
        # Mock run_simulator to avoid actual simulation
        with patch('eudoxia.__main__.run_simulator') as mock_run:
            mock_run.return_value = type('Stats', (), {
                'pipelines_created': 5,
                'pipelines_completed': 3,
                'throughput': 1.5,
                'p99_latency': 0.1,
                'assignments': 8,
                'suspensions': 1,
                'failures': 2,
                'failure_error_counts': {'OOM': 2}
            })()
            
            # Should not raise an exception
            main(['run', params_file])
            
            # Verify run_simulator was called with params dict and no workload
            mock_run.assert_called_once()
            args = mock_run.call_args
            self.assertIsInstance(args[0][0], dict)  # First arg is params dict
            # No workload keyword arg passed (defaults to None in run_simulator)

    # TODO: instead of mocking it out, pass in a simple workload that will have
    # predictable results on a naive scheduler, then make sure outputs are correct
    def test_run_with_params_and_workload(self):
        """Test run command with params and workload files"""
        # Create temporary params file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write('duration = 0.001\nticks_per_second = 100\n')
            params_file = f.name
        
        # Create temporary workload CSV
        csv_data = "\n".join([
            "pipeline_id,arrival_seconds,priority,operator_id,parents,baseline_cpu_seconds,cpu_scaling,memory_gb,storage_read_gb",
            "p1,0.0,QUERY,op1,,1.0,const,,1.0"
        ])
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_data)
            workload_file = f.name
        
        # Mock run_simulator to avoid actual simulation
        with patch('eudoxia.__main__.run_simulator') as mock_run:
            mock_run.return_value = type('Stats', (), {
                'pipelines_created': 1,
                'pipelines_completed': 1,
                'throughput': 10.0,
                'p99_latency': 0.05,
                'assignments': 2,
                'suspensions': 0,
                'failures': 0,
                'failure_error_counts': {}
            })()

            # Should not raise an exception
            main(['run', params_file, '-w', workload_file])

            # Verify run_simulator was called with params dict and workload
            mock_run.assert_called_once()
            args = mock_run.call_args
            self.assertIsInstance(args[0][0], dict)  # First arg is params dict
            self.assertIsNotNone(args[1]['workload'])  # workload keyword arg is not None

    def test_snap_command(self):
        """Test snap command rounds down timestamps to tick boundaries"""
        # Create input workload CSV
        csv_data = "\n".join([
            "pipeline_id,arrival_seconds,priority,operator_id,parents,baseline_cpu_seconds,cpu_scaling,memory_gb,storage_read_gb",
            "p1,0.037,BATCH_PIPELINE,op1,,1.0,const,,1.0",
            "p2,0.053,BATCH_PIPELINE,op1,,1.0,const,,1.0"
        ])
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_data)
            input_file = f.name

        # Create output file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            output_file = f.name

        # Run snap command with ticks_per_second=20
        main(['tools', 'snap', input_file, output_file, '20', '-f'])

        # Read and verify output
        with open(output_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # 0.037 with ticks_per_second=20 -> floor(0.037*20)/20 = 0.0
        # 0.053 with ticks_per_second=20 -> floor(0.053*20)/20 = 0.05
        self.assertEqual(float(rows[0]['arrival_seconds']), 0.0)
        self.assertEqual(float(rows[1]['arrival_seconds']), 0.05)

    def test_jitter_command(self):
        """Test jitter command adds random jitter and sorts by arrival time"""
        # Create input with 4 pipelines: p1(2 rows), p2(1 row), p3(2 rows), p4(1 row)
        csv_data = "\n".join([
            "pipeline_id,arrival_seconds,priority,operator_id,parents,baseline_cpu_seconds,cpu_scaling,memory_gb,storage_read_gb",
            "p1,0.0,BATCH_PIPELINE,op1,,1.0,const,,1.0",
            "p1,,,op2,op1,1.0,const,,1.0",
            "p2,0.0,BATCH_PIPELINE,op1,,1.0,const,,1.0",
            "p3,0.0,BATCH_PIPELINE,op1,,1.0,const,,1.0",
            "p3,,,op2,op1,1.0,const,,1.0",
            "p4,0.0,BATCH_PIPELINE,op1,,1.0,const,,1.0"
        ])
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_data)
            input_file = f.name

        # Create output file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            output_file = f.name

        # Run jitter command
        main(['tools', 'jitter', input_file, output_file, '0.1', '-s', '42', '-f'])

        # Read output
        with open(output_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Count rows per pipeline
        pipeline_rows = {}
        for row in rows:
            pid = row['pipeline_id']
            pipeline_rows[pid] = pipeline_rows.get(pid, 0) + 1

        # Verify we have 4 pipelines
        self.assertEqual(len(pipeline_rows), 4)

        # Verify row counts: p1=2, p2=1, p3=2, p4=1
        self.assertEqual(pipeline_rows['p1'], 2)
        self.assertEqual(pipeline_rows['p2'], 1)
        self.assertEqual(pipeline_rows['p3'], 2)
        self.assertEqual(pipeline_rows['p4'], 1)

        # Verify arrivals are in ascending order
        prev_arrival = -1
        for row in rows:
            if row['arrival_seconds']:
                arrival = float(row['arrival_seconds'])
                self.assertGreaterEqual(arrival, prev_arrival)
                prev_arrival = arrival


if __name__ == '__main__':
    unittest.main()
