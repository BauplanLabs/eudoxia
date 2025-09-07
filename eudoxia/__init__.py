import logging
import logging.config
import tomllib
import sys

logging_config = None
src = "logging.conf"
try:
    with open("logging.conf", 'rb') as fp:
        logging_config = tomlib.load(fp)
except:
    logging_config = {"stream": sys.stdout, "level": logging.DEBUG}
    src = None

logging.basicConfig(**logging_config)
logger = logging.getLogger(__name__)

if src is None:
    logger.info("No logging config passed in; used default basicConfig params")
else:
    logger.info(f"Loaded logging config from {src}")


from . import simulator
from .simulator import *

__all__ = []
__all__ += simulator.__all__
