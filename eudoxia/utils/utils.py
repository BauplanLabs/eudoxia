from enum import Enum

class EudoxiaException(Exception):
    def __init__(self, message):
        super().__init__(message)

class Priority(Enum):
    QUERY = 1
    INTERACTIVE = 2
    BATCH_PIPELINE = 3


class DagShape(Enum):
    LINEAR = "linear"
    BRANCH_IN = "branch_in"
