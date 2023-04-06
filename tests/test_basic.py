from pyprocgraph import PyProcGraphExecutor, run_executor, PyProcGraphWorker, PyProcGraphInput, PyProcGraphOutput
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
        count = 0
        while self.should_work() and count < 10:
            time.sleep(0.1)
            new_str = ''.join(random.choices(string.ascii_lowercase +
                              string.digits, k=30))
            count += 1
            self.output.put(new_str)
        self.terminate_execution()


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


def test_run_executor():
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
    pro_to_process = executor.add_queue("producer_to_processor")
    process_to_con = executor.add_queue("processor_to_consumer")
    producer.output.connect(pro_to_process)
    processor.input.connect(pro_to_process)
    processor.output.connect(process_to_con)
    consumer.input.connect(process_to_con)
    executor.add_worker(producer)
    executor.add_worker(processor)
    executor.add_worker(consumer)
    logging.basicConfig(level=logging.INFO)
    run_executor(executor)
