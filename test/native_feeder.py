import argparse
import hashlib
import time
import logging
import struct

from socketless.channel import Channel, DisconnectedException

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

    challenge, expected_response = ('Tako Public API', 'K')
    listener = (args.address, args.port)
    channel = Channel()
    channel.connect(listener)
    channel.send(challenge)
    channel.flush()
    response = channel.recv()
    if response != expected_response:
        raise Exception('Failed handshake')

    last_time = time.time()
    print 'feeding %s' % repr(listener)
    i = 0
    N = 1024
    while True:
        if time.time() - last_time > 1:
            last_time = time.time()
            print i
        try:
            for j in xrange(N):
                value = sha256(i) * 128
                key = str(i)
                request = 'S'
                fragments = (struct.pack('!cLL', request, len(key), len(value)), key, value)
                message = ''.join(fragments)
                channel.send(message)
                i += 1
            channel.flush()
            for j in xrange(N):
                channel.recv()
        except DisconnectedException, e:
            logging.exception(e)
            exit(-1)
        if args.delay:
            time.sleep(args.delay)
        if args.limit > 0 and i >= args.limit:
            break

if __name__ == '__main__':
    main()
