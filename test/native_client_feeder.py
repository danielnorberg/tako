import argparse
import hashlib
import time
import logging

from syncless import coio

import paths
paths.setup()

from tako.client import Client, ValueNotAvailableException
from tako.utils import debug

def sha256(v):
    sha = hashlib.sha256()
    sha.update(str(v))
    return sha.hexdigest()

counter = 0
def feed(client):
    global counter
    while True:
        key = sha256(str(counter))
        value = sha256(key) * 16
        timestamp = long(time.time() * 1000000.0)
        key = sha256(repr(timestamp))
        counter += 1
        for i in range(3):
            try:
                new_timestamp = client.set(key, timestamp, value)
                if new_timestamp != timestamp:
                    logging.warning('new_timestamp != timestamp (%s != %s)', new_timestamp, timestamp)
                    logging.warning('Retrying...')
                    coio.sleep(1)
                    continue
                fetched_timestamp, fetched_value = client.get(key)
                stat_timestamp = client.stat(key)
                if fetched_timestamp != timestamp:
                    logging.warning('fetched_timestamp != timestamp (%s != %s)', fetched_timestamp, timestamp)
                    logging.warning('Retrying...')
                    coio.sleep(1)
                    continue
                if fetched_value != value:
                    logging.warning('fetched_value != value (%s != %s)', fetched_value, value)
                    logging.warning('Retrying...')
                    coio.sleep(1)
                    continue
                if stat_timestamp != timestamp:
                    logging.warning('stat_timestamp != timestamp (%s != %s)', stat_timestamp, timestamp)
                    logging.warning('Retrying...')
                    coio.sleep(1)
                    continue
                break
            except ValueNotAvailableException, e:
                logging.warning(e)
                logging.warning('Retrying...')
                coio.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="Tako test feeder.")
    parser.add_argument('address')
    parser.add_argument('port', type=int)
    parser.add_argument('-l', '--limit', type=int, default=0)
    parser.add_argument('-d', '--delay', type=float, default=1)
    parser.add_argument('-dbg', '--debug', action='store_true')
    parser.add_argument('-u', '--update', help='Configuration update interval (seconds) default = 300', type=int, default=300)
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    debug.configure_logging('native_client_feeder', level)

    listener = (args.address, args.port)
    client = Client('native_client_feeder', [listener], configuration_update_interval=args.update)
    client.connect()
    while not client.is_connected():
        logging.debug('connected nodes: %d (%d)', client.connected_node_count(), client.total_node_count())
        coio.sleep(0.1)

    print 'feeding cluster coordinated by %s' % repr(listener)
    M = 10000

    for i in xrange(M):
        coio.stackless.tasklet(feed)(client)

    global counter
    while True:
        coio.sleep(1)
        logging.info('counter = %d (%d reqs)', counter, counter * 3)

if __name__ == '__main__':
    main()
