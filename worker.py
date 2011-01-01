#! /usr/bin/python
#
# Copyright (c) 2010 Samuel Tardieu <sam@rfc1149.net>
#
"""Worker for the enhanced request broker."""

import functools, zmq

class Worker:
    """Worker class. One of `process` or `process_multipart` must
    be overriden for this class to be usable."""

    class Error(Exception):
        """Exception raised when something went wrong in the
        worker."""
        pass

    def __init__(self, context, requests_addr, replies_addr):
        """Create a new Worker object getting its data
        from requests_addr and sending its replies to
        replies_addr. It the context is None, a new one
        with one I/O thread will be created."""
        self.context = context or zmq.Context(1)
        self.requests = self.context.socket(zmq.REQ)
        self.requests.connect(requests_addr)
        self.replies = self.context.socket(zmq.PUSH)
        self.replies.connect(replies_addr)

    def request_and_process_task(self):
        """Request a new task to do and call `process_multipart` to
        process it then send the answer."""
        self.requests.send(b"requesting task")
        task = self.requests.recv_multipart()
        answer = task[:1] + self.process_multipart(task[1:])
        self.replies.send_multipart(answer)

    def run(self):
        """Process requests forever."""
        while True:
            request_and_process_task()

    def process_multipart(self, request):
        """Process an incoming multipart request. This method or
        `process` should be overriden. By default, it calls
        `process` after flattening the data. Return a multipart
        answer."""
        return [self.process(functools.reduce(lambda a, b: a + b, request))]

    def process(self):
        """Process a flat request. This method or `process_multipart`
        should be overriden. Return a flat answer."""
        raise Worker.Error("one of `process` or `process_multipart` "
                           "should be overriden")
