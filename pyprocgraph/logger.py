import sys
import time
import logging
from multiprocessing import Queue


class LogPackage(object):
    def __init__(self, source_stage: str, log_message: str, timestamp: float = None):
        self.source_stage = source_stage
        self.log_message = log_message
        self.timestamp = timestamp or time.time()


class FileToQueue(object):
    def __init__(self, source_worker: str, logger_queue: Queue):
        self.source_worker = source_worker
        self.logger_queue = logger_queue
        self.__has_error = False

    def write(self, text):
        try:
            if not self.logger_queue.full():
                unit = LogPackage(self.source_worker, text)
                self.logger_queue.put(unit)
            else:
                print("Logging queue full!")
        except Exception as err:
            if not self.__has_error:
                print(text)
                print(f"Error logging into logger queue: {err}")
                self.__has_error = True

    def flush(self):
        pass


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, level):
       self.logger = logger
       self.level = level
       self.linebuf = ''

    def write(self, buf):
       for line in buf.rstrip().splitlines():
          self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


def CreatePyProcGraphLogger(source_worker: str, queue: Queue, redirect_std=False):
    stream = FileToQueue(source_worker, queue)
    log = logging.getLogger()
    infohandler = logging.StreamHandler(stream)
    infoformatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:'+source_worker+':%(message)s',)
    infohandler.setFormatter(infoformatter)
    log.addHandler(infohandler)
    if redirect_std:
        sys.stdout = StreamToLogger(log, logging.INFO)
        sys.stderr = StreamToLogger(log, logging.ERROR)
