import argparse
import urllib
import gevent, gevent.monkey
import yaml
import simplejson as json
gevent.monkey.patch_all()

def hashs(v):
    return str(hash(str(v)))

def main():
    """docstring for main"""

    parser = argparse.ArgumentParser(description="conf statter.")
    parser.add_argument('-a', '--address', default='localhost')
    parser.add_argument('-p', '--port', type=int, default=8089)
    parser.add_argument('-l', '--limit', type=int, default=0)
    parser.add_argument('-d', '--delay', type=float, default=1)
    args = parser.parse_args()

    url = 'http://%s:%d/configuration' % (args.address, args.port)

    print 'statting configuration at %s' % url
    i = 0
    while True:
        print 'Url: ', url
        try:
            stream = urllib.urlopen(url)
            body = stream.read()
            configuration = json.loads(body)
            # print 'Body: ', repr(body)
            print stream.info()
            print
            print yaml.dump(configuration)
            print
            # print
            stream.close()
            i += 1
        except IOError, e:
            print e
        gevent.sleep(args.delay)
        if args.limit > 0 and i >= args.limit:
            break

if __name__ == '__main__':
    main()
