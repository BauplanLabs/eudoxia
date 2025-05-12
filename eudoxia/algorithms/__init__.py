from .decorators import INIT_ALGOS, SCHEDULING_ALGOS 
from .decorators import register_scheduler, register_scheduler_init
import logging
import sys

logging_config = None
try:
    with open("../logging.conf", 'rb') as fp:
        logging_config = tomlib.load(fp)
except:
    logging_config = {"stream": sys.stdout, "level": logging.DEBUG}

logging.basicConfig(**logging_config)



