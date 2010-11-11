import argparse
import urllib
import gevent, gevent.monkey
gevent.monkey.patch_all()

def main():
	"""docstring for main"""

	parser = argparse.ArgumentParser(description="Hokanjo test reader.")
	parser.add_argument('address')
	parser.add_argument('port', type=int)
	parser.add_argument('-l', '--limit', type=int, default=0)
	parser.add_argument('-d', '--delay', type=float, default=1)
	args = parser.parse_args()

	base_url = 'http://%s:%d/store/' % (args.address, args.port)

	print 'reading %s' % base_url
	i = 0
	while True:
		key = str(i)
		url = base_url + key
		print 'Key: ', key
		try:
			stream = urllib.urlopen(url)
			print 'Value: ', repr(stream.read())
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
