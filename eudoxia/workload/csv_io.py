import csv
from typing import List, Iterator, NamedTuple, Optional, TextIO
from eudoxia.utils import Priority
from .pipeline import Pipeline, Operator, Segment
from .workload import WorkloadReader, PipelineArrival


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
