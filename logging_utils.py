import os
import logging
from pathlib import Path

def get_logger(path, file):
    """Логгирование
    """    
    os.makedirs(path, exist_ok=True)
    log_file = Path(path, file)

    logging.basicConfig(
        level= logging.INFO, #logging.WARNING, #
        format="%(levelname)s: %(asctime)s: %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger()
    handler = logging.FileHandler(log_file, encoding='utf-8')
    handler.setFormatter(logging.Formatter(
        "%(levelname)s: %(asctime)s: %(message)s"
    ))
    logger.addHandler(handler)

    return logger