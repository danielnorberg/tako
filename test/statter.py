import argparse
import urllib
import gevent, gevent.monkey
gevent.monkey.patch_all()

def hashs(v):
    return str(hash(str(v)))

def main():
    """docstring for main"""

    parser = argparse.ArgumentParser(description="Gevent test statter.")
    parser.add_argument('-a', '--address', default='localhost')
    parser.add_argument('-p', '--port', type=int, default=8088)
    parser.add_argument('-l', '--limit', type=int, default=0)
    parser.add_argument('-d', '--delay', type=float, default=1)
    args = parser.parse_args()

    base_url = 'http://%s:%d/stat/' % (args.address, args.port)

    print 'statting %s' % base_url
    i = 0
    while True:
        url = base_url + str(i)
        print 'Url: ', url
        try:
            stream = urllib.urlopen(url)
            print 'Body: ', repr(stream.read())
            print stream.info()
            stream.close()
            i += 1
        except IOError, e:
            print e
        gevent.sleep(args.delay)
        if args.limit > 0 and i >= args.limit:
            break

if __name__ == '__main__':
    main()
