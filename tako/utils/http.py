import urllib, urllib2
import logging

def fetch(url, tries=3):
	"""docstring for fetch"""
	logging.debug('url: %s, tries: %d', url, tries)
	for i in xrange(1, tries):
		try:
			stream = urllib.urlopen(url)
			body = stream.read()
			info = stream.info()
			stream.close()
			return body, info
		except IOError, e:
			logging.error('Error: %s', e)
			return (None, None)

def post(url, value, headers={}):
	"""docstring for post"""
	request = urllib2.Request(url, value, headers)
	stream = urllib2.urlopen(request)
	stream.read()
	stream.close()
