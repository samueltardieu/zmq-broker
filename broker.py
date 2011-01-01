#! /usr/bin/python
#
# Copyright (c) 2010 Samuel Tardieu <sam@rfc1149.net>
#
"""Request broker for 0MQ with timeout/retry handling."""

__author__ = "Samuel Tardieu <sam@rfc1149.net>"

import collections, time, uuid, zmq

def extract_path(request):
    """Separate routing information and data from an incoming request."""
    i = request.index(b"")
    return request[:i+1], request[i+1:]

class Request:
    """A request that has entered the system."""

    def __init__(self, request, timeout, tries):
        """Create a new request with a maximum number of tries.
        After that, the request will be considered a failure."""
        self.path, self.content = extract_path(request)
        self.deadline = None
        self.timeout = timeout
        self.tries = tries

    def update_deadline(self):
        """Update the request deadline if a timeout has been specified."""
        if self.timeout is not None:
            self.deadline = time.time() + self.timeout
        if self.tries is not None:
            self.tries -= 1

    def make_request(self):
        """Return a request prepended by a unique ID to be sent to a worker.
        This also updates the request deadline."""
        self.update_deadline()
        self.id = uuid.uuid1().bytes
        return [self.id] + self.content

    def expired(self):
        """True if the maximum number of tries have been performed."""
        return self.tries == 0

    def timed_out(self):
        """True if we are past the request execution deadline."""
        return self.deadline is not None and time.time() > self.deadline

class Requests:
    """Currently executing requests."""

    def __init__(self, timeout, tries):
        """Create a new empty requests set."""
        if timeout is not None and timeout < 0:
            raise Broker.Error("timeout must be non-negative or None")
        if tries is not None and tries <= 0:
            raise Broker.Error("tries must be positive or None")
        self.waiting = collections.deque()
        self.processing = collections.deque()
        self.bypath = {}
        self.timeout = timeout
        self.tries = tries

    def create_request(self, request):
        """Create a new Request object and make it enter the system."""
        self.enqueue_request(Request(request, self.timeout, self.tries))

    def enqueue_request(self, request):
        """Enqueue a Request object so that it can be picked up by a worker."""
        self.waiting.append(request)

    def process_request(self, request):
        """Set a new request parameter before sending it to a worker."""
        outgoing = request.make_request()
        self.processing.append(request)
        self.bypath[request.id] = request
        return outgoing

    def request_waiting(self):
        """Check if there is any request waiting for a worker."""
        return len(self.waiting) > 0

    def get_request(self):
        """Get the first request in the queue."""
        if self.request_waiting():
            request = self.waiting.popleft()
            return self.process_request(request)
        raise Broker.Error("internal error")

    def check_for_timeouts(self):
        """Check if any request has timed-out. If this is the case,
        requeue it unless the maximum number of tries has been
        reached. Return a list of expired requests."""
        expired = []
        while self.processing and self.processing[0].timed_out():
            request = self.processing.popleft()
            del self.bypath[request.id]
            if request.expired():
                expired.append(request)
            else:
                self.enqueue_request(request)
        return expired

    def dequeue_request(self, answer):
        """Given an answer, remove the corresponding request from
        the queue. Return the answer to send if the request has been
        found in the system, None otherwise (late answer for example)."""
        id, content = answer[0], answer[1:]
        if id in self.bypath:
            request = self.bypath[id]
            self.processing.remove(request)
            del self.bypath[id]
            return request.path + content

    def next_timeout_ms(self):
        """Return the number of milliseconds until the next timeout event,
        or None if there isn't any."""
        if self.processing:
            return 1000 * max(self.processing[0].deadline - time.time(), 0)

class Broker:
    """Broker object with timeout/retry handling."""

    class Error(Exception):
        """Exception raised when something went wrong."""
        pass

    def __init__(self, context, timeout, tries,
                 requesters_addr, workers_query_addr, workers_answer_addr):
        """Create a new broker object. The arguments are the 0MQ context
        to use to create the listening sockets, the timeout in seconds or
        None to wait indefinitely, the number of times a request should
        be attempted or None to retry forever, and the requester,
        worker and worker answer addresses. If context is None, a new
        0MQ context with one I/O thread will be created."""
        self.context = context or zmq.Context(1)

        self.poller = zmq.Poller()

        self.requesters = self.context.socket(zmq.XREP)
        self.requesters.bind(requesters_addr)
        self.poller.register(self.requesters, zmq.POLLIN)

        self.workers_query = self.context.socket(zmq.REP)
        self.workers_query.bind(workers_query_addr)

        self.workers_answer = self.context.socket(zmq.PULL)
        self.workers_answer.bind(workers_answer_addr)
        self.poller.register(self.workers_answer, zmq.POLLIN)

        self.requests = Requests(timeout, tries)

    def run(self):
        """Run forever."""
        while True:
            self.step()

    def step(self):
        """Handle one command (request, worker request, worker answer)
        and act on timeouts."""
        request_waiting = self.requests.request_waiting()
        if request_waiting:
            self.poller.register(self.workers_query, zmq.POLLIN)
        timeout_ms = self.requests.next_timeout_ms()
        for (fd, event) in self.poller.poll(timeout_ms):
            if fd == self.requesters:
                self.requests.create_request(self.requesters.recv_multipart())
            if fd == self.workers_answer:
                answer = self.workers_answer.recv_multipart()
                outgoing = self.requests.dequeue_request(answer)
                if outgoing is not None:
                    self.requesters.send_multipart(outgoing)
            if fd == self.workers_query:
                self.workers_query.recv()
                self.workers_query.send_multipart(self.requests.get_request())
        for failure in self.requests.check_for_timeouts():
            self.requesters.send_multipart(failure.path + [b""])
        if request_waiting:
            self.poller.unregister(self.workers_query)

if __name__ == '__main__':
    import optparse
    parser = optparse.OptionParser()
    parser.add_option("-t", "--tries", dest="tries",
                      help="Number of tries (default: infinite)",
                      action="store", type="int", default=None)
    parser.add_option("-T", "--timeout", dest="timeout",
                      help="Timeout in seconds (default: none)",
                      action="store", type="float", default=None)
    parser.usage = "%prog [options] requestsAddr workersAddr answersAddr"
    (options, args) = parser.parse_args()
    if len(args) != 3:
        parser.error("incorrect number of arguments")
    b = Broker(zmq.Context(1), options.timeout, options.tries, *args)
    b.run()
