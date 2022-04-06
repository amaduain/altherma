import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import date, datetime


##GLOBAL VARIABLES####

daikin_db_name = "daikin"

log_level = logging.INFO
interval = 10 #Seconds


def create_logger(log_file_name, log_level):
    """
        Create the logger for the script.

       :returns: logger, log_handler Objects properly configured.
       :rtype: tuple
    """
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    log_handler = RotatingFileHandler(log_file_name, maxBytes=20000000,
                                      backupCount=5)
    log_handler.setFormatter(formatter)
    logger.setLevel(log_level)
    # Enable the screen logging.
    logger.addHandler(log_handler)
    console = logging.StreamHandler()
    console.setLevel(log_level)
    logger.addHandler(console)
    return logger, log_handler


if __name__ == '__main__':
    logger, log_handler = create_logger("./log/daiking.log",log_level)
    logger.info("Starting daiking measurements...")
