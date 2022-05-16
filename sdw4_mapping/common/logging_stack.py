import logging
import traceback
from typing import List


class LoggingHandlerStackHandler(logging.StreamHandler):
    def emit(self, record):
        if record.levelno == logging.WARNING or record.levelno == logging.ERROR or record.levelno == logging.CRITICAL:
            tbfs: List[traceback.FrameSummary] = traceback.extract_stack()
            tbfs=[f for f in tbfs[0:-7] if '/site-packages/' not in f.filename and '<frozen ' not in f.filename]
            tbf=tbfs[len(tbfs) -1]
            record.stack_caller = f'\nFile "{tbf.filename}", line {tbf.lineno}, in {tbf.name}'
        else:
            record.stack_caller = ''
        super().emit(record)
