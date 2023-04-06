from multiprocessing import managers, Manager, Event
from multiprocessing.queues import JoinableQueue, Queue
from uuid import uuid4
from time import time, sleep
import logging
from .worker import PyProcGraphWorker
from .exceptions import PyProcGraphException
from .communication import PyProcGraphQueue
import signal


class PyProcGraphExecutor(object):
    def __init__(self):
        self.__ctx = managers.get_context()
        self.__manager = Manager()
        self.__running = Event()
        self.__profiling = Event()
        self.session_id = str(uuid4())
        self.logging_queue = Queue(ctx=self.__ctx)
        self.shared_state = self.__manager.dict()
        self.workers = {}
        self.queues = {}
        self.start_ts = None

    def add_worker(self, worker: PyProcGraphWorker):
        self.workers[worker.name] = worker
        worker.join_executor(self)

    def add_queue(self, name: str, maxsize=10000):
        if name in self.queues:
            raise PyProcGraphException(f"Queue with name {name} already exists, could not add to executor.")
        queue = PyProcGraphQueue(name=name, ctx=self.__ctx, maxsize=maxsize)
        self.queues[name] = queue
        return queue

    def start(self):
        self.start_ts = time()
        self.resume()
        for name, worker in self.workers.items():
            logging.debug(f"Executor is starting worker {name}")
            worker.start()

    def join(self, timeout: int = None):
        for name, worker in self.workers.items():
            logging.debug(f"joining worker {name}")
            worker.join(timeout)
            logging.debug(f"joining worker {name} complete")

    def terminate(self):
        for name, worker in self.workers.items():
            logging.debug(f"terminating worker {name}")
            worker.terminate()

    def all_alive(self):
        return all(self.alive_status())

    def any_alive(self):
        return any(self.alive_status())

    def alive_status(self):
        return [s.is_alive() for s in self.workers.values()]

    def stop(self):
        self.__running.clear()

    def is_running(self):
        return self.__running.is_set()

    def resume(self):
        self.__running.set()

    def shutdown_gracefully(self, timeout: float = 1):
        logging.info("Stopping executor workers")
        self.stop()

        logging.info("Joining terminated workers")
        self.join(timeout=timeout)

        if self.any_alive():
            logging.warning("Join timeout reached, terminating all remaining workers")
            self.terminate()
        else:
            logging.info("All workers shutdown gracefully")

        self.logging_queue.cancel_join_thread()
        for queue in self.queues.values():
            queue.cancel_join_thread()

    def shutdown(self):
        logging.info("Stopping executor workers")
        self.stop()

        logging.info("Joining terminated workers")
        self.join(timeout=0.01)

        if self.any_alive():
            logging.warning("Join timeout reached, terminating all remaining workers")
            self.terminate()
        else:
            logging.info("All remaining workers shutdown gracefully")

        self.logging_queue.cancel_join_thread()
        for queue in self.queues.values():
            queue.cancel_join_thread()

    def enable_profiling(self):
        self.__profiling.set()

    def is_profiling(self):
        return self.__profiling.is_set()


def run_executor(executor: PyProcGraphExecutor, timeout: float = 1, monitor_freq: float = 1, log_file_name: str = None):
    def handle_sigterm(signum, frame):
        logging.info("SIGTERM triggered termination")
        executor.shutdown_gracefully(timeout)
        exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    log_file = None
    if log_file_name:
        log_file = open(log_file_name, "w")
    executor.start()
    try:
        sleep(monitor_freq)
        alive = executor.alive_status()

        while all(alive) and executor.is_running():
            if log_file:
                while not executor.logging_queue.empty():
                    mesg = executor.logging_queue.get_nowait()
                    log_file.write(mesg.log_message)
            alive = executor.alive_status()
            sleep(monitor_freq)

        if not all(alive) and executor.is_running():
            for worker in executor.workers.values():
                if not worker.is_alive():
                    logging.error("Worker %s (PID=%d) exited non-gracefully" % (worker.name, worker.pid))
            executor.shutdown()
            exit(1)
    except KeyboardInterrupt:
        logging.info("SIGINT triggered termination")
        executor.shutdown_gracefully()
        exit(0)
    except Exception as err:
        logging.error(f"Fatal exception in main executor loop: {err}")
        executor.shutdown()
        exit(1)
    finally:
        if log_file:
            log_file.close()
        executor.shutdown_gracefully(timeout)
