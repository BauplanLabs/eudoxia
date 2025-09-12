import csv
from typing import List, Iterator, NamedTuple, Optional, TextIO
from eudoxia.utils import Priority
from .pipeline import Pipeline, Operator, Segment
from .workload import WorkloadReader, PipelineArrival, Workload


class CSVOperatorRow(NamedTuple):
    """
    Columns expected by a CSVWorkloadReader
    """
    pipeline_id: str
    arrival_seconds: float
    priority: str
    operator_id: str
    parents: str  # semicolon-separated list of parent operator_ids
    baseline_cpu_seconds: float
    cpu_scaling: str
    memory_gb: Optional[float]
    storage_read_gb: float


class CSVWorkloadReader(WorkloadReader):
    """
    Reads CSV files containing operator/pipeline definitions for workloads.
    
    Design Assumption: Each CSV row represents one Operator containing exactly one Segment.
    This 1:1 mapping simplifies the format while still allowing complex pipeline DAGs
    to be constructed via the parent-child relationships between operators.
    
    Expected CSV columns:
    - pipeline_id: identifier for the pipeline
    - arrival_seconds: when this pipeline arrives in the system
    - priority: pipeline priority ("INTERACTIVE", "QUERY", "BATCH_PIPELINE") - only set in first row of each pipeline, blank in subsequent rows
    - operator_id: identifier for the operator within the pipeline  
    - parents: semicolon-separated list of parent operator_ids (empty for root)
    - baseline_cpu_seconds: CPU time for one processor
    - cpu_scaling: scaling function name (e.g., "linear3", "const")
    - memory_gb: memory usage in GB (optional, will default to storage_read_gb)
    - storage_read_gb: storage to read in GB
    """

    def __init__(self, file_handle: TextIO):
        """Initialize CSV reader with an open file handle"""
        self.file_handle = file_handle

    def batch_by_arrival(self) -> Iterator[List[PipelineArrival]]:
        """
        Generator that yields batches of PipelineArrival objects grouped by arrival time.
        
        Yields:
            List[PipelineArrival]: All pipeline arrivals that happen at the same time
        """
        current_batch = []
        current_arrival_seconds = None
        
        for pipeline_arrival in self.batch_by_pipeline():
            if current_arrival_seconds is None:
                # First pipeline
                current_arrival_seconds = pipeline_arrival.arrival_seconds
                current_batch = [pipeline_arrival]
            elif pipeline_arrival.arrival_seconds == current_arrival_seconds:
                # Same arrival time, add to current batch
                current_batch.append(pipeline_arrival)
            else:
                # New arrival time, yield current batch and start new one
                yield current_batch
                current_arrival_seconds = pipeline_arrival.arrival_seconds
                current_batch = [pipeline_arrival]
        
        # Yield the last batch if it exists
        if current_batch:
            yield current_batch

    def batch_by_pipeline(self) -> Iterator[PipelineArrival]:
        """
        Generator that yields PipelineArrival objects (pipeline + arrival time).
                
        Yields:
            PipelineArrival: Pipeline object with its arrival time
        """
        reader = csv.DictReader(self.file_handle)
        
        current_batch = []
        current_pipeline_id = None
        
        for row_dict in reader:
            row = self._parse_row(row_dict)
            
            if current_pipeline_id is None:
                # First row
                current_pipeline_id = row.pipeline_id
                current_batch = [row]
            elif row.pipeline_id == current_pipeline_id:
                # Same pipeline, add to current batch
                current_batch.append(row)
            else:
                # New pipeline, yield current batch and start new one
                arrival_seconds = current_batch[0].arrival_seconds
                pipeline = self.create_pipeline_from_batch(current_batch)
                yield PipelineArrival(arrival_seconds, pipeline)
                
                current_pipeline_id = row.pipeline_id
                current_batch = [row]
        
        # Yield the last batch if it exists
        if current_batch:
            arrival_seconds = current_batch[0].arrival_seconds
            pipeline = self.create_pipeline_from_batch(current_batch)
            yield PipelineArrival(arrival_seconds, pipeline)
    
    def create_pipeline_from_batch(self, batch: List[CSVOperatorRow]) -> Pipeline:
        """
        Create a Pipeline object from a batch of rows (all same pipeline_id).
        
        Constructs the DAG structure based on operator_id and parents relationships.
        Each row becomes an Operator containing exactly one Segment.
        """
        if not batch:
            raise ValueError("Cannot create pipeline from empty batch")
        
        # All rows should have the same pipeline_id, priority only set in first row
        pipeline_id = batch[0].pipeline_id
        priority_str = batch[0].priority
        priority = Priority[priority_str]
        
        # Verify all rows have the same pipeline_id and priority pattern (first row has priority, others blank)
        for i, row in enumerate(batch):
            if row.pipeline_id != pipeline_id:
                raise ValueError(f"All rows in batch must have same pipeline_id: {row.pipeline_id} != {pipeline_id}")
            if i == 0:
                # First row must have priority set
                if not row.priority:
                    raise ValueError(f"First row of pipeline {pipeline_id} must have priority set")
            else:
                # Subsequent rows must have blank priority
                if row.priority:
                    raise ValueError(f"Only first row of pipeline {pipeline_id} should have priority set, found '{row.priority}' in row {i+1}")
        
        # Create the pipeline
        pipeline = Pipeline(priority)
        
        # Create operators and segments (1:1 relationship)
        operators = {}  # operator_id -> Operator
        
        for row in batch:
            # Create the single segment for this operator
            segment = Segment(
                baseline_cpu_seconds=row.baseline_cpu_seconds,
                cpu_scaling=row.cpu_scaling,
                memory_gb=row.memory_gb,
                storage_read_gb=row.storage_read_gb
            )
            
            # Create the operator with exactly one segment
            operator = Operator()
            operator.add_segment(segment)
            operators[row.operator_id] = operator
        
        # Build the DAG structure
        for row in batch:
            operator = operators[row.operator_id]
            
            if not row.parents:
                # root
                pipeline.values.add_node(operator)
            else:
                # other
                parent_ids = [pid.strip() for pid in row.parents.split(';') if pid.strip()]
                parent_operators = [operators[pid] for pid in parent_ids]
                pipeline.values.add_node(operator, parent_operators)

        return pipeline

    def _parse_row(self, row_dict: dict) -> CSVOperatorRow:
        """Convert a dictionary row from DictReader to CSVOperatorRow namedtuple"""
        return CSVOperatorRow(
            pipeline_id=row_dict['pipeline_id'],
            arrival_seconds=float(row_dict['arrival_seconds']),
            priority=row_dict.get('priority', '').strip(),
            operator_id=row_dict['operator_id'],
            parents=row_dict.get('parents', '').strip(),
            baseline_cpu_seconds=float(row_dict['baseline_cpu_seconds']),
            cpu_scaling=row_dict['cpu_scaling'],
            memory_gb=float(row_dict['memory_gb']) if row_dict.get('memory_gb') else None,
            storage_read_gb=float(row_dict['storage_read_gb'])
        )


class CSVWorkloadWriter:
    """Writes workload data to CSV format"""
    
    def __init__(self, file_handle: TextIO):
        """Initialize CSV writer with an open file handle"""
        self.file_handle = file_handle
        self.writer = csv.DictWriter(file_handle, fieldnames=[
            'pipeline_id', 'arrival_seconds', 'priority', 'operator_id', 'parents',
            'baseline_cpu_seconds', 'cpu_scaling', 'memory_gb', 'storage_read_gb'
        ])
        self.writer.writeheader()

    def write_row(self, row: CSVOperatorRow):
        """Write a single CSVOperatorRow to the file"""
        self.writer.writerow({
            'pipeline_id': row.pipeline_id,
            'arrival_seconds': row.arrival_seconds,
            'priority': row.priority,
            'operator_id': row.operator_id,
            'parents': row.parents,
            'baseline_cpu_seconds': row.baseline_cpu_seconds,
            'cpu_scaling': row.cpu_scaling,
            'memory_gb': row.memory_gb if row.memory_gb is not None else '',
            'storage_read_gb': row.storage_read_gb
        })


class WorkloadTraceGenerator:
    """Helper class to convert Workload instances to CSVOperatorRow objects"""
    
    def __init__(self, workload, ticks_per_second: int, duration_secs: float):
        """Initialize with a Workload instance and simulation parameters"""
        self.workload = workload
        self.ticks_per_second = ticks_per_second
        self.tick_length_secs = 1.0 / ticks_per_second
        self.max_ticks = int(duration_secs * ticks_per_second)
    
    def _pipeline_to_rows(self, pipeline: Pipeline, pipeline_id: str, arrival_seconds: float) -> Iterator[CSVOperatorRow]:
        """Convert a single pipeline to CSVOperatorRow objects"""
        # Convert pipeline DAG to flat list of operators with parent relationships
        operators = list(pipeline.values.node_lookup.values())
        
        for i, operator in enumerate(operators):
            operator_id = f"op{i+1}"
            
            # Determine parent operator IDs
            parent_ids = []
            for parent in operator.parents:
                parent_idx = operators.index(parent)
                parent_ids.append(f"op{parent_idx+1}")
            parents_str = ';'.join(parent_ids)
            
            # Get segment info (assuming 1:1 operator-segment relationship)
            segments = operator.get_segments()
            if len(segments) != 1:
                raise ValueError(f"Expected exactly 1 segment per operator, but operator {operator_id} in pipeline {pipeline_id} has {len(segments)} segments")
            segment = segments[0]
            
            # Find scaling function name by reverse lookup
            cpu_scaling = None
            for name, func in Segment.SCALING_FUNCS.items():
                if func == segment.scaling_func:
                    cpu_scaling = name
                    break
            
            if cpu_scaling is None:
                raise ValueError(f"Could not find scaling function name for operator {operator_id} in pipeline {pipeline_id}. Unknown scaling function: {segment.scaling_func}")
            
            # Priority only in first row of each pipeline
            priority = pipeline.priority.name if i == 0 else ''
            
            yield CSVOperatorRow(
                pipeline_id=pipeline_id,
                arrival_seconds=arrival_seconds,
                priority=priority,
                operator_id=operator_id,
                parents=parents_str,
                baseline_cpu_seconds=segment.baseline_cpu_seconds,
                cpu_scaling=cpu_scaling,
                memory_gb=segment.memory_gb,
                storage_read_gb=segment.storage_read_gb
            )
    
    def generate_rows(self) -> Iterator[CSVOperatorRow]:
        """Generate CSVOperatorRow objects from the workload"""
        pipeline_counter = 0
        
        for tick in range(self.max_ticks):
            arrival_seconds = tick * self.tick_length_secs
            pipelines = self.workload.run_one_tick()
            
            for pipeline in pipelines:
                pipeline_counter += 1
                pipeline_id = f"p{pipeline_counter}"
                
                yield from self._pipeline_to_rows(pipeline, pipeline_id, arrival_seconds)
