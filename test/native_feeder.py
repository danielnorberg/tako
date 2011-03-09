import argparse
import hashlib
import time
import logging
import struct
import random

from syncless import coio

import paths
paths.setup()

from socketless.service import Client
from tako.protocols import PublicNodeServiceProtocol

def sha256(v):
    sha = hashlib.sha256()
    sha.update(str(v))
    return sha.hexdigest()

def main():
    """docstring for main"""

    parser = argparse.ArgumentParser(description="Tako test feeder.")
    parser.add_argument('address')
    parser.add_argument('port', type=int)
    parser.add_argument('-l', '--limit', type=int, default=0)
    parser.add_argument('-d', '--delay', type=float, default=1)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)

    listener = (args.address, args.port)
    client = Client(listener, PublicNodeServiceProtocol())
    while not client.is_connected():
        coio.sleep(0.01)

    last_time = time.time()
    print 'feeding %s' % repr(listener)
    i = 0
    N = 1000
    while True:
        if time.time() - last_time > 1:
            last_time = time.time()
            print i
        collector = client.set_collector(N)
        for j in xrange(N):
            key = sha256('%d:%d' % (i, j))
            value = sha256(key) * 16
            client.set_async(collector, key, value)
            i += 1
        collector.collect()
        if not client.is_connected():
            exit(-1)
        if args.delay:
            coio.sleep(args.delay)
        if args.limit > 0 and i >= args.limit:
            break

if __name__ == '__main__':
    main()
