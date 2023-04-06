import abc
import time
import psutil
import logging
import cProfile
import os
import pstats
from multiprocessing import Process
from threading import Thread
from queue import Empty
from .logger import CreatePyProcGraphLogger
from .communication import PyProcGraphQueue


class PyProcGraphWorkerBase(Process):
    def __init__(self, name):
        super().__init__(name=name, group=None)


class SharedDictWrapper(object):
    def __init__(self, worker):
        """
        Wrapper on shared dict object that intercepts any connectivity related
        exceptions and passes them directly to the worker who owns this dict.
        :param worker:
        """
        self._worker = worker

    def __getitem__(self, key):
        try:
            return self._worker._executor.shared_state.get(key, None)
        except BrokenPipeError or ConnectionRefusedError:
            logging.warning("Lost connection to executor, aborting worker %s", self._worker.name)
            self._worker.abort()

    def __setitem__(self, key, value):
        try:
            self._worker._executor.shared_state[key] = value
        except BrokenPipeError or ConnectionRefusedError:
            logging.warning("Lost connection to executor, aborting worker %s", self._worker.name)
            self._worker.abort()

    def keys(self):
        try:
            return self._worker._executor.shared_state.keys()
        except BrokenPipeError or ConnectionRefusedError:
            logging.warning("Lost connection to executor, aborting worker %s", self._worker.name)
            self._worker.abort()

    def items(self):
        try:
            return self._worker._executor.shared_state.items()
        except BrokenPipeError or ConnectionRefusedError:
            logging.warning("Lost connection to executor, aborting worker %s", self._worker.name)
            self._worker.abort()

    def values(self):
        try:
            return self._worker._executor.shared_state.values()
        except BrokenPipeError or ConnectionRefusedError:
            logging.warning("Lost connection to executor, aborting worker %s", self._worker.name)
            self._worker.abort()


class PyProcGraphIO(object):
    direction: int

    def __init__(self, name: str, owner: PyProcGraphWorkerBase):
        """
        Generic IO interface class. This will be inherited by Input and Output objects
        :param name:
        :param owner:
        """
        self.name = name
        self.owner = owner
        self.queue = None
        self.item_count = 0

    def connect(self, queue: PyProcGraphQueue):
        self.queue = queue
        if self.direction == 0:
            self.queue.track_consumer(self)
        else:
            self.queue.track_producer(self)

    def _safe_get_queue(self) -> PyProcGraphQueue:
        if not self.queue:
            raise Exception("port not connected")
        return self.queue

    def is_connected(self) -> bool:
        return not self.queue is None

    def full(self) -> bool:
        queue = self._safe_get_queue()
        return queue.full()

    def can_be_used(self) -> bool:
        return self.is_connected() and not self.full()

    def qsize(self):
        queue = self._safe_get_queue()
        return queue.qsize()

    def compute_stats(self, tick_span: float) -> dict:
        pass


class PyProcGraphInput(PyProcGraphIO):
    def __init__(self, name: str, owner: PyProcGraphWorkerBase):
        """
        Input object. This is a wrapper around a multiprocessing queue, allows the PyProcGraphWorker
        to interface with any input queues connected with it.
        :param name:
        :param owner:
        """
        super().__init__(name, owner)
        self.direction = 0
        self.item_count = 0

    def get(self, block: bool = True, timeout: float = None):
        queue = self._safe_get_queue()
        item = queue.get(block=block, timeout=timeout)
        self.item_count += 1
        return item

    def get_or_wait(self, timeout: float):
        queue = self._safe_get_queue()
        try:
            item = queue.get(True, timeout)
            self.item_count += 1
            return item
        except Empty:
            return

    def get_or_continue(self):
        queue = self._safe_get_queue()
        if not queue.empty():
            try:
                item = queue.get_nowait()
            except Empty:
                return
            self.item_count += 1
            return item
        return

    def yield_until_empty(self):
        queue = self._safe_get_queue()
        while not queue.empty():
            try:
                item = queue.get_nowait()
            except Empty:
                return
            self.item_count += 1
            yield item

    def drop_all_but_last(self):
        queue = self._safe_get_queue()
        while queue.qsize() > 1:
            try:
                queue.get(timeout=.1)
            except Empty:
                return
            self.item_count += 1
        unit = queue.get()
        self.item_count += 1
        return unit

    def drop_all_but_last_or_wait(self, timeout: float):
        queue = self._safe_get_queue()
        while queue.qsize() > 1:
            try:
                queue.get(timeout=.1)
            except Empty:
                return
            self.item_count += 1
        try:
            item = queue.get(timeout=timeout)
            self.item_count += 1
            return item
        except Empty:
            return


class PyProcGraphOutput(PyProcGraphIO):
    def __init__(self, name: str, owner: PyProcGraphWorkerBase):
        """
        Output object. This is a wrapper around a multiprocessing queue, allows the PyProcGraphWorker
        to interface with any output queues connected with it.
        :param name:
        :param owner:
        """
        super().__init__(name, owner)
        self.direction = 1
        self.item_count += 1

    def put(self, item, block: bool = True):
        queue = self._safe_get_queue()
        queue.put(item, block=block)
        self.item_count += 1

    def put_if_not_full(self, item):
        queue = self._safe_get_queue()
        if not queue.full():
            queue.put(item)
            self.item_count += 1


class PyProcGraphWorker(PyProcGraphWorkerBase):
    def __init__(self, name, stat_interval: float = 1):
        """
        This is the generic abstract worker object. Any real worker will derive from this class.
        :param name: name of the worker node for easier identificaiton
        :param stat_interval: interval between two consequtive statistics probes
        """
        super().__init__(name)
        self._executor = None
        self.__aborted = False
        self.__restarted = 0
        self._is_working = False
        self.__stat_interval = stat_interval
        self.shared_state = SharedDictWrapper(self)
        self.redirect_std = False
        self.__step = 0
        self.__max_tdelta = 0
        self.__min_tdelta = 100
        self.__avg_tdelta = 0
        self.__t_start = None
        self.__n_steps = 0
        self.__last_t = None
        self.__absolute_t_start = None
        self.__process_info = None
        self._stat_thread = None

    def _register_execution(self):
        """
        Internal method used to gather timing statistics
        :return:
        """
        t = time.time()
        tdelta = t - self.__last_t
        if self.__step == 0:
            self.__t_start = t
        self.__step += 1
        self.__n_steps += 1
        if tdelta > self.__max_tdelta:
            self.__max_tdelta = tdelta
        if tdelta < self.__min_tdelta:
            self.__min_tdelta = tdelta
        self.__avg_tdelta = 0.999 * self.__avg_tdelta + 0.001 * tdelta
        self.__last_t = time.time()

    def yield_io(self):
        """
        Search the instance for any members that are instances of PyProcGraphIO.
        This is to gather comunication statistics. Direct members are found as well as if they
        are items in a list or a dictionary. Instances hidden deeper in the object will not be found.
        :return:
        """
        ret_val = []
        for att in dir(self):
            if att != "sentinel" and att != "__dict__":
                member = getattr(self, att)
                if isinstance(member, PyProcGraphIO):
                    ret_val.append(member)
                if isinstance(member, list):
                    for item in member:
                        if isinstance(item, PyProcGraphIO):
                            ret_val.append(item)
                if isinstance(member, dict):
                    for item in member.values():
                        if isinstance(item, PyProcGraphIO):
                            ret_val.append(item)
        return ret_val

    def __stat_thread_run(self):
        """
        Executed in a separate theread, used to periodically collect
        execution statistics.
        :return:
        """
        stat_dict = {}
        connections = self.yield_io()
        while self.__should_work():
            time.sleep(self.__stat_interval)
            with self.__process_info.oneshot():
                stat_dict["cpu_total"] = self.__process_info.cpu_percent()
                stat_dict["cpu_user"] = self.__process_info.cpu_times().user
                stat_dict["cpu_system"] = self.__process_info.cpu_times().system
                stat_dict["cpu_iowait"] = self.__process_info.cpu_times().iowait
                stat_dict["mem_rss"] = self.__process_info.memory_info().rss
                stat_dict["mem_vms"] = self.__process_info.memory_info().vms
                stat_dict["steps_total"] = self.__step
                stat_dict["steps_since_last"] = self.__n_steps
                stat_dict["stat_interval"] = self.__stat_interval
                stat_dict["steps_per_sec"] = self.__n_steps * 1.0 / self.__stat_interval
                stat_dict["max_loop_time_ms"] = self.__max_tdelta * 1000
                stat_dict["min_loop_time_ms"] = self.__min_tdelta * 1000
                stat_dict["avg_loop_time_ms"] = self.__avg_tdelta * 1000
                self.__max_tdelta = 0
                self.__min_tdelta = 100
                self.__t_start = time.time()
                self.__n_steps = 0
                stat_dict["connections"] = []
                for connection in connections:
                    stat_dict["connections"].append({"name": connection.name, "item_count": connection.item_count})
            self.shared_state[f'{self.name}_stats'] = stat_dict

    def abort(self):
        """
        Abort execution.
        :return:
        """
        self.__aborted = True

    def join_executor(self, executor):
        self._executor = executor

    def _should_restart_on_exception(self, err: Exception) -> bool:
        """
        To be overriden by deriving class, pick exceptions which
        when thrown inside execution code will lead to restart of the
        worker. Unless specified here, an uncaptured exception thrown in the
        work method will cause the worker to exit.
        :param err:
        :return:
        """
        return False

    @abc.abstractmethod
    def initialize(self):
        """
        Needs to be implemented by the end object (can do nothing if initialization is not recommended). This
        will execute in the child process so any process specific initializaiton (such as e.g. interfacing with
        hardware, CUDA etc.) should be performed here and not in the object constructor.
        :return:
        """
        pass

    @abc.abstractmethod
    def work(self):
        """
        Needs to be implemented by the end object, will contain the main work loop.
        :return:
        """
        pass

    @abc.abstractmethod
    def cleanup(self):
        """
        Needs to be implemented by the end object, any process specific cleanup needs to go here.
        :return:
        """
        pass

    def __should_work(self):
        """
        Determines when the work loop should be executed
        :return:
        """
        return self._executor.is_running() and not self.__aborted

    def should_work(self):
        """
        Like above only marks loop execution times for statistics gathering.
        Use this one inside your work method.
        :return:
        """
        self._register_execution()
        return self.__should_work()

    def terminate_execution(self):
        self._executor.stop()

    def _start_profiling(self):
        """
        Internal method starting a profiler session
        :return:
        """
        self._profiler = cProfile.Profile()
        self._profiler.enable()
        logging.info("Worker profiling is enabled. File: " + self.name + "_PID:" + str(os.getpid()) + ".prof will be generated.")

    def _end_profiling(self):
        """
        Internal method, ending profiler session
        :return:
        """
        self._profiler.disable()
        stats = pstats.Stats(self._profiler)
        stats.dump_stats(self.name + "_PID:" + str(os.getpid()) + ".prof")
        logging.info("Finished profiling. File: " + self.name + "_PID:" + str(os.getpid()) + ".prof was generated.")

    def __work_forever(self):
        """
        Internal method executed in the child process.
        :return:
        """
        while self.__should_work():
            try:
                self.__last_t = time.time()
                logging.info(f"Worker {self.name}  will start with PID: {self.__process_info.pid}")
                self.initialize()
                self._is_working = True
                if self._executor.is_profiling():
                    self._start_profiling()
                self.work()
            except KeyboardInterrupt:
                self.__aborted = True
                break
            except Exception as exc:
                logging.exception(f"Critical ERROR in worker {self.name} : {exc}")
                should_restart = self._should_restart_on_exception(exc)
                if should_restart:
                    self.__restarted += 1
                    logging.warning(f"Restarting worker (N={self.__restarted}) {self.name}  after critical exception")
            finally:
                logging.info(f"Worker {self.name} with PID: {self.__process_info.pid} is finishing execution")
                if self._executor.is_profiling():
                    self._end_profiling()
                self.cleanup()
                self._is_working = False
                logging.info(f"Worker {self.name} with PID: {self.__process_info.pid} cleaned up.")

    def run(self):
        """
        This will be run in the child process. Launches everything and executes __work_forever loop.
        :return:
        """
        CreatePyProcGraphLogger(self.name, self._executor.logging_queue, redirect_std=self.redirect_std)
        self.__process_info = psutil.Process()
        self._stat_thread = Thread(target=self.__stat_thread_run)
        self._stat_thread.daemon = True
        self.__absolute_t_start = time.time()
        self.__last_t = time.time()
        self._stat_thread.start()
        self.__work_forever()
