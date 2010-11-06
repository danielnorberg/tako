import argparse
import urllib2
import gevent, gevent.monkey
import hashlib
import time
gevent.monkey.patch_all()

def sha256(v):
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

	last_time = time.time()
	print 'feeding %s' % base_url
	i = 0
	while True:
		key = str(i)
		value = sha256(i)
		url = base_url + key
		if time.time() - last_time > 1:
			last_time = time.time()
			print i
			print 'Posting to %s' % url
		try:
			headers = {
				# 'X-TimeStamp':time.time()
			}
			request = urllib2.Request(url, value, headers)
			stream = urllib2.urlopen(request)
			stream.close()
			i += 1
		except IOError:
			print 'Failed...'
		gevent.sleep(args.delay)
		if args.limit > 0 and i >= args.limit:
			break

if __name__ == '__main__':
	main()
