import logging
import sys
from .workload import Workload, WorkloadGenerator
from .scheduler import Scheduler
from .executor import Executor
# from .pipeline import Operator, Pipeline, Segment
# from .assignment import Assignment, Failure, Suspend

logging_config = None
try:
    with open("../logging.conf", 'rb') as fp:
        logging_config = tomlib.load(fp)
except:
    logging_config = {"stream": sys.stdout, "level": logging.DEBUG}

logging.basicConfig(**logging_config)

