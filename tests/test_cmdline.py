import io
import tempfile
import unittest
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
                'oom_failures': 0,
                'throughput': 1.5,
                'p99_latency': 0.1
            })()
            
            # Should not raise an exception
            main(['run', params_file])
            
            # Verify run_simulator was called with params file and no workload
            mock_run.assert_called_once()
            args = mock_run.call_args
            self.assertEqual(args[0][0], params_file)  # First arg is params file
            self.assertIsNone(args[1]['workload'])  # workload keyword arg is None

    # TODO: instead of mocking it out, pass in a simple workload that will have
    # predictable results on a naive scheduler, then make sure outputs are correct
    def test_run_with_params_and_workload(self):
        """Test run command with params and workload files"""
        # Create temporary params file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write('duration = 0.001\ntick_length_secs = 0.01\n')
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
                'oom_failures': 0,
                'throughput': 10.0,
                'p99_latency': 0.05
            })()

            # Should not raise an exception
            main(['run', params_file, '-w', workload_file])

            # Verify run_simulator was called with params file and workload
            mock_run.assert_called_once()
            args = mock_run.call_args
            self.assertEqual(args[0][0], params_file)  # First arg is params file
            self.assertIsNotNone(args[1]['workload'])  # workload keyword arg is not None


if __name__ == '__main__':
    unittest.main()
