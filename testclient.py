#! /usr/bin/python
#

import threading, zmq

# Compatibility between Python 2 and Python 3
def to_bytes(n):
    try:
        return bytes(str(n))
    except:
        return bytes(str(n), "ascii")

class TestClient(threading.Thread):

    def __init__(self, context, address, id):
        super(TestClient, self).__init__()
        self.context = context
        self.id = id
        self.sock = context.socket(zmq.REQ)
        self.sock.connect(address)
        self.log("starting")

    def log(self, msg):
        print("Client %d: %s" % (self.id, msg))

    def run(self):
        for data in range(self.id * 100, self.id * 100 + 100):
            self.log("sending %d" % data)
            self.sock.send(to_bytes(data))
            answer = self.sock.recv()
            if answer == b"":
                self.log("failure")
            else:
                self.log("answer: %s" % answer)

def start_clients(nthreads, address):
    context = zmq.Context(1)
    threads = [TestClient(context, address, n) for n in range(nthreads)]
    [t.start() for t in threads]
    [t.join() for t in threads]

if __name__ == '__main__':
    import optparse, sys
    parser = optparse.OptionParser()
    parser.add_option("-n", "--threads", dest="nthreads",
                      help="Number of threads (default: 1)",
                      action="store", type="int", default=1)
    parser.usage = "%prog [options] serverAddr"
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")
    start_clients(options.nthreads, args[0])

