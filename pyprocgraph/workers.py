from .worker import PyProcGraphWorker, PyProcGraphInput, PyProcGraphOutput
from .communication import PyProcGraphQueue
from queue import Full
import time
import pprint
import logging

class StatPrinter(PyProcGraphWorker):
    def __init__(self, name: str, stat_interval: float = 1, update_interval: float = 1):
        super().__init__(name, stat_interval)
        self.update_interval = update_interval

    def initialize(self):
        pass

    def cleanup(self):
        pass

    def work(self):
        while self.should_work():
            for key, val in self.shared_state.items():
                if isinstance(key, str) and key.endswith("_stats"):
                    print(key)
                    pprint.pprint(val)
            time.sleep(self.update_interval)


class Splitter(PyProcGraphWorker):
    def __init__(self, name: str, stat_interval: float = 1):
        """
        A node that splits messages coming through input queue and sends them across
        all output queues.
        :param name:
        :param stat_interval:
        """
        super().__init__(name, stat_interval)
        self.input = PyProcGraphInput(name="Input", owner=self)
        self.outputs = []

    def connect_output(self, output: PyProcGraphQueue = None):
        idx = len(self.outputs)
        self.outputs.append(PyProcGraphOutput(name="Output_%d" % idx, owner=self))
        self.outputs[-1].connect(output)

    def initialize(self):
        pass

    def cleanup(self):
        pass

    def work(self):
        while self.should_work():
            incoming = self.input.get_or_wait(1)
            if incoming:
                for i, output in enumerate(self.outputs):
                    try:
                        output.put(incoming)
                    except Full:
                        logging.debug(f"Splitter worker {self.name}: output {i} is full, cannot enqueue more items.")
                        continue

class Funnel(PyProcGraphWorker):
    def __init__(self, name: str, stat_interval: float = 1):
        """
        A node that accumulated messages from multiple queues and funnels them into a single output
        :param name:
        :param stat_interval:
        """
        super().__init__(name, stat_interval)
        self.inputs = []
        self.output = PyProcGraphOutput(name="Output", owner=self)

    def connect_input(self, input_queue: PyProcGraphQueue = None):
        idx = len(self.inputs)
        self.inputs.append(PyProcGraphInput(name="Input_%d" % idx, owner=self))
        self.inputs[-1].connect(input_queue)

    def initialize(self):
        pass

    def cleanup(self):
        pass

    def work(self):
        while self.should_work():
            for i, input_o in enumerate(self.inputs):
                incoming = input_o.get_or_continue()
                if incoming:
                    try:
                        self.output.put(incoming)
                    except Full:
                        logging.debug(f"Funnel worker {self.name}: output {i} is full, cannot enqueue more items.")
                        continue
