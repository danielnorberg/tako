import argparse
import hashlib
import time
import logging
import gc
import random
import sys

from syncless import coio

import paths
paths.setup()

from tako.utils import debug

from tako.client import Client

import refcounts

def sha256(v):
    sha = hashlib.sha256()
    sha.update(str(v))
    return sha.hexdigest()

counter = 0
def feed(client):
    global counter
    while True:
        key = sha256(int(random.randint(0, sys.maxint)))
        value = sha256(key) * 16
        timestamp = long(time.time() * 1000000.0)
        logging.debug(('sending', counter, key, timestamp))
        client.set_value(key, value, timestamp)
        logging.debug(('done', counter, key, timestamp))
        counter += 1


def main():
    parser = argparse.ArgumentParser(description="Tako test feeder.")
    parser.add_argument('address')
    parser.add_argument('port', type=int)
    parser.add_argument('-l', '--limit', type=int, default=0)
    parser.add_argument('-d', '--delay', type=float, default=1)
    parser.add_argument('-dbg', '--debug', action='store_true')
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.ERROR
    debug.configure_logging('native_client_feeder', level)

    listener = (args.address, args.port)
    client = Client([listener])
    while not client.is_connected():
        logging.debug('connected nodes: %d (%d)', client.connected_node_count(), client.total_node_count())
        coio.sleep(0.1)

    # last_time = time.time()
    print 'feeding cluster coordinated by %s' % repr(listener)
    M = 1000

    for i in xrange(M):
        coio.stackless.tasklet(feed)(client)

    global counter
    last_counter = 0
    while True:
        coio.sleep(1)
        print counter

    # i = 0
    # N = 1000
    # while True:
    #     if time.time() - last_time > 1:
    #         last_time = time.time()
    #         print i
    #     for j in xrange(N):
    #         key = sha256('%d:%d' % (i, j))
    #         value = sha256(key) * 16
    #         timestamp = long(time.time() * 1000000.0)
    #         # coio.stackless.tasklet(client.set)(key, value, timestamp)
    #         # client.set(key, value, timestamp)
    #         i += 1
    #     if args.delay:
    #         coio.sleep(args.delay)
    #         while coio.stackless.getruncount() > N:
    #             coio.stackless.schedule()
    #     if i > 40000:
    #         refcounts.print_top_100()
    #         coio.sleep(100)
    #     if args.limit > 0 and i >= args.limit:
    #         break

if __name__ == '__main__':
    main()
