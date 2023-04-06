from pyprocgraph import PyProcGraphExecutor, run_executor, PyProcGraphWorker, PyProcGraphInput, PyProcGraphOutput
from pyprocgraph.workers import StatPrinter, Splitter, Funnel
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
    More sophisticated compute graph:

                                    [Processor]
    [Producer] -> [Splitter 10x] -> [Processor] -> [Funnel 10/] -> [Consumer]
                                      ... 10x
                                    [Processor]

    With StatPrinter running in parallel to print statistics every 10s.

    Executing a dummy task of taking a string and turning it to uppercase
    :return:
    """

    executor = PyProcGraphExecutor()
    producer = MyStrGen("producer")
    pro_to_splitter = executor.add_queue("producer_to_splitter")
    producer.output.connect(pro_to_splitter)
    splitter = Splitter("splitter")
    splitter.input.connect(pro_to_splitter)
    funnel = Funnel("Funnel")
    num_processors = 10
    for i in range(num_processors):
        splitter_to_process = executor.add_queue("splitter_to_processor_%d" % i)
        process_to_funnel = executor.add_queue("processor_to_funnel_%d" % i)
        splitter.connect_output(splitter_to_process)
        processor = MyUpperProcess("processor_%i" % i)
        processor.input.connect(splitter_to_process)
        processor.output.connect(process_to_funnel)
        executor.add_worker(processor)
        funnel.connect_input(process_to_funnel)

    stat = StatPrinter("stat", update_interval=10)
    funnel_to_cons = executor.add_queue("funnel_to_consumer")
    funnel.output.connect(funnel_to_cons)
    consumer = MyPrinterProcess("Printer")
    consumer.input.connect(funnel_to_cons)
    executor.add_worker(producer)
    executor.add_worker(splitter)
    executor.add_worker(funnel)
    executor.add_worker(consumer)
    executor.add_worker(stat)
    # TODO: Uncomment to start gathering profiler data
    # executor.enable_profiling()
    logging.basicConfig(level=logging.INFO)
    run_executor(executor)


if __name__ == "__main__":
    run()
