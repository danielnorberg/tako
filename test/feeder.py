import argparse
import urllib3
import hashlib
import time

import logging

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

	host_url = 'http://%s:%d/' % (args.address, args.port)
	http_pool = urllib3.connection_from_url(host_url)

	last_time = time.time()
	print 'feeding %s' % host_url
	i = 0
	while True:
		key = str(i)
		value = sha256(i)
		url = '/store/' + key
		if time.time() - last_time > 1:
			last_time = time.time()
			print i
			print 'Posting to %s' % url
		try:
			r = http_pool.urlopen('POST', url, body=value)
			i += 1
		except IOError:
			print 'Failed...'
		if args.delay:
			time.sleep(args.delay)
		if args.limit > 0 and i >= args.limit:
			break

if __name__ == '__main__':
	main()
