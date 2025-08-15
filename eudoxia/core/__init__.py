import logging
import sys
from .executor import Executor

logging_config = None
try:
    with open("../logging.conf", 'rb') as fp:
        logging_config = tomlib.load(fp)
except:
    logging_config = {"stream": sys.stdout, "level": logging.DEBUG}

logging.basicConfig(**logging_config)

