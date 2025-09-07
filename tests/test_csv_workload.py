import io
import unittest
from eudoxia.workload.csv_io import CSVWorkloadReader
from eudoxia.workload.workload import WorkloadTrace
from eudoxia.utils import Priority


class TestCSVWorkloadReader(unittest.TestCase):
    
    def test_reader_basic(self):
        """Test CSVWorkloadReader with two pipelines, one multi-operator"""
        csv_data = "\n".join([
            "pipeline_id,arrival_seconds,priority,operator_id,parents,baseline_cpu_seconds,cpu_scaling,memory_gb,storage_read_gb",
            "p1,0.0,QUERY,op1,,15.0,linear3,,35.0",
            "p1,0.0,,op2,op1,10.0,sqrt,,20.0",
            "p2,1.0,BATCH_PIPELINE,op1,,5.0,const,,45.0"
        ])
        
        with io.StringIO(csv_data) as f:
            reader = CSVWorkloadReader(f)
            
            arrival_batches = list(reader.batch_by_arrival())
            self.assertEqual(len(arrival_batches), 2)
            
            # First batch at time 0.0 has one pipeline (p1) with 2 operators
            batch1 = arrival_batches[0]
            self.assertEqual(len(batch1), 1)
            self.assertEqual(batch1[0].arrival_seconds, 0.0)
            self.assertEqual(len(batch1[0].pipeline.values.node_ids), 2)
            
            # Second batch at time 1.0 has one pipeline (p2) with 1 operator
            batch2 = arrival_batches[1]
            self.assertEqual(len(batch2), 1)
            self.assertEqual(batch2[0].arrival_seconds, 1.0)
            self.assertEqual(len(batch2[0].pipeline.values.node_ids), 1)


class TestWorkloadTrace(unittest.TestCase):
    
    def test_trace_simple(self):
        """Test WorkloadTrace delivers pipelines at correct ticks"""
        csv_data = "\n".join([
            "pipeline_id,arrival_seconds,priority,operator_id,parents,baseline_cpu_seconds,cpu_scaling,memory_gb,storage_read_gb",
            "p1,0.0,QUERY,op1,,15.0,linear3,,35.0",
            "p2,2.5,BATCH_PIPELINE,op1,,5.0,const,,45.0"
        ])
        
        with io.StringIO(csv_data) as f:
            reader = CSVWorkloadReader(f)
            trace = WorkloadTrace(reader, tick_length_secs=1.0)
            
            # Tick 0: get p1
            pipelines = trace.run_one_tick()
            self.assertEqual(len(pipelines), 1)
            
            # Tick 1: nothing
            pipelines = trace.run_one_tick()
            self.assertEqual(len(pipelines), 0)

            # Tick 2: nothing
            pipelines = trace.run_one_tick()
            self.assertEqual(len(pipelines), 0)

            # Tick 3: get p2 because it arrives at 2.5 ticks
            pipelines = trace.run_one_tick()
            self.assertEqual(len(pipelines), 1)

    def test_trace_multiple_same_tick(self):
        """Test multiple pipelines on same tick"""
        csv_data = "\n".join([
            "pipeline_id,arrival_seconds,priority,operator_id,parents,baseline_cpu_seconds,cpu_scaling,memory_gb,storage_read_gb",
            "p1,1.2,QUERY,op1,,15.0,linear3,,35.0",
            "p2,1.8,BATCH_PIPELINE,op1,,5.0,const,,45.0"
        ])
        
        with io.StringIO(csv_data) as f:
            reader = CSVWorkloadReader(f)
            trace = WorkloadTrace(reader, tick_length_secs=1.0)
            
            # Tick 0: nothing
            pipelines = trace.run_one_tick()
            self.assertEqual(len(pipelines), 0)
            
            # Tick 1: nothing
            pipelines = trace.run_one_tick()
            self.assertEqual(len(pipelines), 0)
            
            # Tick 2: both pipelines
            pipelines = trace.run_one_tick()
            self.assertEqual(len(pipelines), 2)


if __name__ == '__main__':
    unittest.main()
