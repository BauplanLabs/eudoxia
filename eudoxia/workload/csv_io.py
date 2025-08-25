import csv
from typing import List, Iterator, NamedTuple, Optional, TextIO
from eudoxia.utils import Priority
from .pipeline import Pipeline, Operator, Segment


class OperatorRow(NamedTuple):
    """
    Represents a single row from the CSV file.
    
    Note: Each row corresponds to an Operator, which contains exactly one Segment.
    This is a design assumption that simplifies the CSV format - rather than having
    separate rows for segments within operators, we assume a 1:1 relationship.
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


class CSVWorkloadReader:
    """
    Reads CSV files containing operator/pipeline definitions for workloads.
    
    Design Assumption: Each CSV row represents one Operator containing exactly one Segment.
    This 1:1 mapping simplifies the format while still allowing complex pipeline DAGs
    to be constructed via the parent-child relationships between operators.
    
    Expected CSV columns:
    - pipeline_id: identifier for the pipeline
    - priority: pipeline priority ("INTERACTIVE", "QUERY", "BATCH_PIPELINE")
    - operator_id: identifier for the operator within the pipeline  
    - parents: semicolon-separated list of parent operator_ids (empty for root)
    - baseline_cpu_seconds: CPU time for one processor
    - cpu_scaling: scaling function name (e.g., "linear3", "const")
    - memory_gb: memory usage in GB (optional, will default to storage_read_gb)
    - storage_read_gb: storage to read in GB
    
    Memory Efficient: Processes CSV files row-by-row without loading entire file into memory.
    """
    
    def __init__(self, file_handle: TextIO):
        """Initialize CSV reader with an open file handle"""
        self.file_handle = file_handle
    
    def _parse_row(self, row_dict: dict) -> OperatorRow:
        """Convert a dictionary row from DictReader to OperatorRow namedtuple"""
        return OperatorRow(
            pipeline_id=row_dict['pipeline_id'],
            priority=row_dict['priority'],
            operator_id=row_dict['operator_id'],
            parents=row_dict.get('parents', '').strip(),
            baseline_cpu_seconds=float(row_dict['baseline_cpu_seconds']),
            cpu_scaling=row_dict['cpu_scaling'],
            memory_gb=float(row_dict['memory_gb']) if row_dict.get('memory_gb') else None,
            storage_read_gb=float(row_dict['storage_read_gb'])
        )
    
    def batch_by_pipeline(self) -> Iterator[List[OperatorRow]]:
        """
        Generator that yields batches of rows grouped by pipeline_id.
        
        Memory efficient: Only holds one pipeline's worth of rows in memory at a time.
        Assumes rows with the same pipeline_id are consecutive in the file.
        
        Yields:
            List[OperatorRow]: All rows belonging to a single pipeline
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
                yield current_batch
                current_pipeline_id = row.pipeline_id
                current_batch = [row]
        
        # Yield the last batch if it exists
        if current_batch:
            yield current_batch
    
    def create_pipeline_from_batch(self, batch: List[OperatorRow]) -> Pipeline:
        """
        Create a Pipeline object from a batch of rows (all same pipeline_id).
        
        Constructs the DAG structure based on operator_id and parents relationships.
        Each row becomes an Operator containing exactly one Segment.
        """
        if not batch:
            raise ValueError("Cannot create pipeline from empty batch")
        
        # All rows should have the same pipeline_id and priority
        pipeline_id = batch[0].pipeline_id
        priority_str = batch[0].priority
        priority = Priority[priority_str]
        
        # Verify all rows have the same pipeline_id and priority
        for row in batch:
            if row.pipeline_id != pipeline_id:
                raise ValueError(f"All rows in batch must have same pipeline_id: {row.pipeline_id} != {pipeline_id}")
            if row.priority != priority_str:
                raise ValueError(f"All rows in batch must have same priority: {row.priority} != {priority_str}")
        
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
            
            if row.parents:
                # Has parents - add to DAG with parent relationships
                parent_ids = [pid.strip() for pid in row.parents.split(';') if pid.strip()]
                parent_operators = [operators[pid] for pid in parent_ids]
                pipeline.values.add_node(operator, parent_operators)
            else:
                # No parents - this is a root node
                pipeline.values.add_node(operator)
        
        return pipeline
    
    def pipelines(self) -> Iterator[Pipeline]:
        """
        Generator that yields Pipeline objects one at a time.
        
        Memory efficient: Only holds one pipeline in memory at a time.
        Each pipeline is constructed from consecutive rows with the same pipeline_id.
        
        Yields:
            Pipeline: A single pipeline constructed from its batch of rows
        """
        for batch in self.batch_by_pipeline():
            yield self.create_pipeline_from_batch(batch)