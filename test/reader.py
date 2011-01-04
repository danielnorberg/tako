import argparse
import urllib3
import time

def main():
	"""docstring for main"""

	parser = argparse.ArgumentParser(description="Tako test reader.")
	parser.add_argument('address')
	parser.add_argument('port', type=int)
	parser.add_argument('-l', '--limit', type=int, default=0)
	parser.add_argument('-d', '--delay', type=float, default=1)
	args = parser.parse_args()

	host_url = 'http://%s:%d/' % (args.address, args.port)
	http_pool = urllib3.connection_from_url(host_url)

	last_time = time.time()
	print 'reading %s' % host_url
	i = 0
	while True:
		key = str(i)
		path = '/store/' + key
		verbose = time.time() - last_time > 1
		if verbose:
			last_time = time.time()
			print i
			print 'Reading from %s' % path
		try:
			r = http_pool.urlopen('GET', path)
			if verbose:
				print 'Value: ', repr(r.data)
				print r.status
				print r.headers
			i += 1
		except IOError, e:
			print e
		if args.delay:
			time.sleep(args.delay)
		if args.limit > 0 and i >= args.limit:
			break

if __name__ == '__main__':
	main()
