import io
import unittest
from eudoxia.workload.csv_io import CSVWorkloadReader, CSVWorkloadWriter, WorkloadTraceGenerator


class TestCSVWorkloadWriter(unittest.TestCase):
    
    def test_roundtrip_csv_consistency(self):
        """Test that reading a CSV, generating a trace, and writing back produces the same CSV"""
        # Original CSV data
        original_csv = "\n".join([
            "pipeline_id,arrival_seconds,priority,operator_id,parents,baseline_cpu_seconds,cpu_scaling,memory_gb,storage_read_gb",
            "p1,0.0,QUERY,op1,,15.0,linear3,,35.0",
            "p1,0.0,,op2,op1,10.0,sqrt,,20.0",
            "p2,1.0,BATCH_PIPELINE,op1,,5.0,const,,45.0",
            "p2,1.0,,op2,op1,8.0,linear7,,25.0",
            "p2,1.0,,op3,op2,12.0,squared,,30.0",
            "p3,2.0,INTERACTIVE,op1,,20.0,const,,40.0"
        ])
        
        # Read original CSV and create workload trace
        # Keep StringIO alive for the duration of the workload trace
        csv_input = io.StringIO(original_csv)
        reader = CSVWorkloadReader(csv_input)
        workload = reader.get_workload(tick_length_secs=1.0)
        
        # Generate new CSV from the workload trace
        # Need to run long enough to capture all arrivals (p3 arrives at 2.5s)
        trace_generator = WorkloadTraceGenerator(workload, tick_length_secs=1.0, duration_secs=4.0)
        
        output_csv = io.StringIO()
        writer = CSVWorkloadWriter(output_csv)
        writer.write_header()
        
        for row in trace_generator.generate_rows():
            writer.write_row(row)
        
        # Compare the CSVs
        generated_csv = output_csv.getvalue()
        
        # Normalize line endings for comparison
        original_normalized = original_csv.strip().replace('\r\n', '\n')
        generated_normalized = generated_csv.strip().replace('\r\n', '\n')
        
        # Print for debugging if they don't match
        if original_normalized != generated_normalized:
            print("Original:")
            print(repr(original_normalized))
            print("Generated:")
            print(repr(generated_normalized))
        
        self.assertEqual(original_normalized, generated_normalized)


if __name__ == '__main__':
    unittest.main()