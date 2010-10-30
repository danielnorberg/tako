import argparse
import urllib
import gevent, gevent.monkey
gevent.monkey.patch_all()

def hashs(v):
	return str(hash(str(v)))

def main():
	"""docstring for main"""

	parser = argparse.ArgumentParser(description="Gevent test reader.")
	parser.add_argument('-a', '--address', default='localhost')
	parser.add_argument('-p', '--port', type=int, default=8088)
	parser.add_argument('-l', '--limit', type=int, default=0)
	parser.add_argument('-d', '--delay', type=float, default=1)
	args = parser.parse_args()

	url = 'http://%s:%d/store/' % (args.address, args.port)

	print 'reading %s' % url
	i = 0
	while True:
		print i
		try:
			stream = urllib.urlopen(url + str(i))
			print stream.read()
			stream.close()
			i += 1
		except IOError:
			print 'Failed...'
		gevent.sleep(args.delay)
		if args.limit > 0 and i >= args.limit:
			break

if __name__ == '__main__':
	main()
