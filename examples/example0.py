from pyprocgraph import PyProcGraphExecutor, run_executor, PyProcGraphWorker, PyProcGraphInput, PyProcGraphOutput
from pyprocgraph.workers import StatPrinter
import random
import string
import time
import logging


class MyUpperProcess(PyProcGraphWorker):
    def __init__(self, name):
        super().__init__(name)
        self.input = PyProcGraphInput(name="Input", owner=self)
        self.output = PyProcGraphOutput(name="Output", owner=self)

    def initialize(self):
        pass

    def cleanup(self):
        pass

    def work(self):
        while self.should_work():
            item = self.input.get_or_wait(timeout=1.0)
            if not item:
                continue
            modified = item.upper()
            self.output.put(modified)


class MyStrGen(PyProcGraphWorker):
    def __init__(self, name):
        super().__init__(name)
        self.output = PyProcGraphOutput(name="Output", owner=self)

    def initialize(self):
        pass

    def cleanup(self):
        pass

    def work(self):
        while self.should_work():
            time.sleep(0.1)
            new_str = ''.join(random.choices(string.ascii_lowercase +
                              string.digits, k=30))
            self.output.put(new_str)


class MyPrinterProcess(PyProcGraphWorker):
    def __init__(self, name):
        super().__init__(name)
        self.input = PyProcGraphInput(name="Input", owner=self)

    def initialize(self):
        pass

    def cleanup(self):
        pass

    def work(self):
        while self.should_work():
            item = self.input.get_or_wait(timeout=1.0)
            if not item:
                continue
            print(item, flush=True)


def run():
    """
    Minimalistic compute graph:

    [Producer] -> [Processor] -> [Consumer]

    With StatPrinter running in parallel to print statistics every 10s.

    Executing a dummy task of taking a string and turning it to uppercase
    :return:
    """
    executor = PyProcGraphExecutor()
    producer = MyStrGen("producer")
    processor = MyUpperProcess("processor")
    consumer = MyPrinterProcess("consumer")
    stat = StatPrinter("stat", update_interval=10)
    pro_to_process = executor.add_queue("producer_to_processor")
    process_to_con = executor.add_queue("processor_to_consumer")
    producer.output.connect(pro_to_process)
    processor.input.connect(pro_to_process)
    processor.output.connect(process_to_con)
    consumer.input.connect(process_to_con)
    executor.add_worker(producer)
    executor.add_worker(processor)
    executor.add_worker(consumer)
    executor.add_worker(stat)
    # TODO: Uncomment to start gathering profiler data
    # executor.enable_profiling()
    logging.basicConfig(level=logging.INFO)
    run_executor(executor)


if __name__ == "__main__":
    run()
