import logging
import os
import sys
import atexit
from typing import List, Union

import pandas as pd

from sdw4_mapping.common.logging_stack import LoggingHandlerStackHandler

logger_initialized = False


def read_data(data_path: str, usecols: List[str]) -> Union[pd.DataFrame, None]:
    """
    Reads data from single csv file, returns pd.DataFrame.
    If file doesn't exist, ithrows an exception and warns user, returns None.
    Uses String Converter to convert all the data to the string format.
    Columns to read can be specified by "usecols" parameter.
    """
    if data_path is not None:
        if os.path.exists(data_path):
            df = pd.read_csv(filepath_or_buffer=data_path, usecols=usecols, converters=StringConverter())
        else:
            logging.error(f'Wrong data path: {data_path}')
            df = None
    else:
        df = None
    return df


def save_to_csv(df: pd.DataFrame, domain_name: str, output_folder: str) -> None:
    """
    Saves csv to output folder. Name of the output file is a domain name.
    If the folder to save doensn't exist, creates it. Prints to stdout if csv was saved.
    """
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    df.to_csv(os.path.join(output_folder, domain_name + '.csv'), index=False,encoding='utf-8-sig')
    logging.info(f'Saved {domain_name} domain output ({len(df)} rows) to {domain_name}.csv')


class StringConverter(dict):
    """
    Converter for pd.read_csv method to convert all the values in csv to string format while reading.
    """

    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str


def init_logger(level=logging.INFO) -> logging:
    """
    Initialize logger from python logging library. Logs errors to stderr and debug to stdout.
    """
    global logger_initialized
    if logger_initialized:
        return
    logger_initialized = True

    logging.basicConfig(level=level)
    formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s%(stack_caller)s')

    # Fix new line issue after moving it at the beginning of the formatter line and removing terminator char
    def on_exit():
        sys.stderr.write('\n')
        sys.stdout.write('\n')

    atexit.register(on_exit)

    debug_handler = LoggingHandlerStackHandler(sys.stdout)
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(formatter)
    debug_handler.propagate = False

    error_handler = LoggingHandlerStackHandler(sys.stderr)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    error_handler.propagate = False

    logger = logging.getLogger()
    # Remove default handlers
    for handler in logger.handlers:
        logger.removeHandler(handler)
    logger.propagate = False
    logger.addHandler(debug_handler)
    logger.getChild('').addHandler(error_handler)

    return logger


def replace_nan_to_None(series: pd.Series) -> pd.Series:
    return series.where(pd.notnull(series), None)
