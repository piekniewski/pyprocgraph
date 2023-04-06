from multiprocessing.queues import Queue


class PyProcGraphQueue(Queue):
    def __init__(self, name: str, ctx, maxsize: int = None, something=None):
        super().__init__(ctx=ctx, maxsize=maxsize)
        self.name = name
        self.producers = []
        self.consumers = []

    def track_producer(self, output_connection):
        self.producers.append(output_connection)

    def track_consumer(self, input_connection):
        self.consumers.append(input_connection)


