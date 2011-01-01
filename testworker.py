#! /usr/bin/python
#

import random, worker

class TestWorker(worker.Worker):

    class Dropped(Exception):
        pass

    def __init__(self, context, requests_addr, replies_addr, drop_rate):
        super(TestWorker, self).__init__(context, requests_addr, replies_addr)
        self.drop_rate = drop_rate

    def process(self, data):
        print("Received request %s" % data)
        if random.random() < self.drop_rate:
            print("  Dropping it")
            raise TestWorker.Dropped()
        else:
            print("  Answering it")
            return b"-" + data

    def run(self):
        while True:
            try:
                print("Asking for a request")
                self.request_and_process_task()
            except TestWorker.Dropped:
                pass

if __name__ == '__main__':
    import optparse, sys
    parser = optparse.OptionParser()
    parser.usage = "%prog [options] workersAddr answersAddr dropRate"
    (option, args) = parser.parse_args()
    if len(args) != 3:
        parser.error("incorrect number of arguments")
    TestWorker(None, args[0], args[1], float(args[2])).run()
