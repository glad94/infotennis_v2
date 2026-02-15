import logging
import sys

def get_logger(name: str = "infotennis_v2"):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
