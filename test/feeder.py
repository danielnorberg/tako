import argparse
import urllib
import gevent, gevent.monkey
import hashlib
gevent.monkey.patch_all()

def hashs(v):
	sha = hashlib.sha256()
	sha.update(str(v))
	return sha.hexdigest()

def main():
	"""docstring for main"""

	parser = argparse.ArgumentParser(description="Gevent test feeder.")
	parser.add_argument('-a', '--address', default='localhost')
	parser.add_argument('-p', '--port', type=int, default=8088)
	parser.add_argument('-l', '--limit', type=int, default=0)
	parser.add_argument('-d', '--delay', type=float, default=1)
	args = parser.parse_args()

	base_url = 'http://%s:%d/store/' % (args.address, args.port)

	print 'feeding %s' % base_url
	i = 0
	while True:
		print i
		try:
			url = base_url + str(i)
			print 'Posting to %s' % url
			stream = urllib.urlopen(url, hashs(i))
			stream.close()
			i += 1
		except IOError:
			print 'Failed...'
		gevent.sleep(args.delay)
		if args.limit > 0 and i >= args.limit:
			break

if __name__ == '__main__':
	main()
