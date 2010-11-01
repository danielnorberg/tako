import gevent, gevent.monkey
gevent.monkey.patch_all()
import urllib, urllib2
import argparse
import yaml
import logging
import debug
import os, sys
import email.utils
import time
import httpserver

from store import Store
from configuration import Configuration

class BadRequest(object):
	"""docstring for BadRequest"""
	def __init__(self, description=''):
		super(BadRequest, self).__init__()
		self.description = description

	def __str__(self):
		"""docstring for __str__"""
		return repr(self)

	def __repr__(self):
		"""docstring for __repr__"""
		return "BadRequest('%s')" % self.description

class NodeServer(httpserver.HttpServer):
	def __init__(self, node_id, db_file, configuration):
		super(NodeServer, self).__init__()
		self.id = node_id
		self.file = db_file
		self.store = Store(self.file)
		self.store.open()
		self.configuration = configuration
		self.deployment = configuration.active_deployment
		self.read_repair_enabled = self.configuration.active_deployment.read_repair_enabled
		self.node = self.deployment.nodes[self.id]
		self.siblings = self.deployment.siblings(self.id)
		self.handlers = (
			('/store/', {'GET':self.store_GET, 'POST':self.store_POST}),
			('/internal/', {'GET':self.internal_GET, 'POST':self.internal_POST}),
			('/stat/', {'GET':self.stat_GET}),
		)

	def quote(self, key):
		"""docstring for quote"""
		return urllib.quote_plus(key, safe='/&')

	def unquote(self, path):
		"""docstring for unquote"""
		return urllib.unquote_plus(path)

	def store_GET(self, start_response, path, body, env):
		logging.debug("path: %s", path)
		key = self.unquote(path)
		value, timestamp = self.store.get(key)

		if self.read_repair_enabled:
			value, timestamp = self.read_repair(key, value, timestamp)

		if value:
			start_response('200 OK', [
				('Content-Type', 'application/octet-stream'),
				('Last-Modified', email.utils.formatdate(timestamp)),
				('X-TimeStamp', repr(timestamp)),
			])
			return value
		else:
			start_response('404 Not Found', [])
			return ['']

	def store_POST(self, start_response, path, body, env):
		key = self.unquote(path)
		value = body.read()
		timestamp = self.get_timestamp(env)
		logging.debug("key: %s, timestamp: %s", repr(key), repr(timestamp))
		self.store.set(key, value, timestamp)
		self.propagate(key, value, timestamp)
		start_response('200 OK', [
			('Content-Type', 'application/octet-stream'),
			('X-TimeStamp', repr(timestamp)),
		])
		return ['']

	def internal_GET(self, start_response, path, body, env):
		"""docstring for internal_GET"""
		logging.debug("path: %s", path)
		key = self.unquote(path)
		value, timestamp = self.store.get(key)
		if value:
			start_response('200 OK', [
				('Content-Type', 'application/octet-stream'),
				('Last-Modified', email.utils.formatdate(timestamp)),
				('X-TimeStamp', repr(timestamp)),
			])
			return [value]
		else:
			start_response('404 Not Found', [])
			return ['']

	def internal_POST(self, start_response, path, body, env):
		"""docstring for internal_POST"""
		key = self.unquote(path)
		value = body.read()
		timestamp = self.get_timestamp(env)
		logging.debug("key: %s, timestamp: %s", repr(key), repr(timestamp))
		self.store.set(key, value, timestamp)
		start_response('200 OK', [
			('Content-Type', 'application/octet-stream'),
			('X-TimeStamp', repr(timestamp)),
		])
		return ['']

	def stat_GET(self, start_response, path, body, env):
		"""docstring for stat_GET"""
		logging.debug("path: %s", path)
		key = self.unquote(path)
		value, timestamp = self.store.get(key)
		logging.debug("path: %s, timestamp: %s", repr(path), repr(timestamp))
		if value:
			start_response('200 OK', [
				('Content-Type', 'application/octet-stream'),
				('Last-Modified', email.utils.formatdate(timestamp)),
				('X-TimeStamp', repr(timestamp)),
			])
			return [repr(timestamp)]
		else:
			start_response('404 Not Found', [])
			return ['']

	def send(self, key, value, timestamp, node):
		logging.debug('key: %s, timestamp: %s, node: %s', repr(key), repr(timestamp), node)
		path = self.quote(key)
		try:
			url = node.internal_url() + path
			request = urllib2.Request(url, value, {'X-TimeStamp': repr(timestamp)})
			stream = urllib2.urlopen(request)
			stream.read()
			stream.close()
		except IOError, e:
			logging.error('Failed to post data to sibling (%s): %s', node, e)

	def fetch_value(self, key, node):
		"""docstring for fetch_value"""
		logging.debug('key: %s, node: %s', repr(key), node)
		path = self.quote(key)
		try:
			url = node.internal_url() + path
			stream = urllib.urlopen(url)
			body = stream.read()
			stream.close()
			return body
		except IOError, e:
			logging.error('Error: %s', e)
		except ValueError, e:
			logging.error('Error: %s', e)
		return (None, node)

	def fetch_timestamp(self, key, node):
		"""docstring for fetch_timestamp"""
		logging.debug('key: %s, node: %s', repr(key), node)
		path = self.quote(key)
		try:
			url = node.stat_url() + path
			logging.debug('url: %s', url)
			stream = urllib.urlopen(url)
			info = stream.info()
			stream.close()
			timestamp_string = info.get('X-TimeStamp', None)
			timestamp = timestamp_string and float(timestamp_string)
			logging.debug('key: %s, node: %s, timestamp: %s', repr(key), node, repr(timestamp))
			return (timestamp, node)
		except IOError, e:
			logging.error('Error: %s', e)
		except ValueError, e:
			logging.error('Error: %s', e)
		return (None, node)

	def fetch_timestamps(self, key):
		"""docstring for fetch_timestamps"""
		logging.debug('key: %s', repr(key))
		neighbour_buckets = self.configuration.find_neighbour_buckets(key, self.node)
		neighbour_bucket_nodes = [node for bucket in neighbour_buckets for node in bucket]
		target_nodes = self.siblings + neighbour_bucket_nodes
		logging.debug('target nodes: %s', target_nodes)
		greenlets = [gevent.spawn(self.fetch_timestamp, key, node) for node in target_nodes]
		gevent.joinall(greenlets)
		timestamps = sorted(greenlet.value for greenlet in greenlets)
		return timestamps

	def get_timestamp(self, env):
		"""docstring for get_timestamp"""
		try:
			timestamp = float(env.get('HTTP_X_TIMESTAMP', time.time()))
			return timestamp
		except ValueError:
			raise BadRequest()

	def propagate(self, key, value, timestamp, target_nodes = []):
		"""docstring for propagate"""
		logging.debug('key: %s, value:%s', repr(key), repr(value))
		if not target_nodes:
			neighbour_buckets = self.configuration.find_neighbour_buckets(key, self.node)
			neighbour_bucket_nodes = [node for bucket in neighbour_buckets for node in bucket]
			target_nodes = self.siblings + neighbour_bucket_nodes
		greenlets = [gevent.spawn(self.send, key, value, timestamp, node) for node in target_nodes]
		logging.debug('target nodes: %s' % repr(target_nodes))
		gevent.joinall(greenlets)

	def read_repair(self, key, value, timestamp):
		"""docstring for read_repair"""
		remote_timestamps = self.fetch_timestamps(key)
		logging.debug('remote: %s', remote_timestamps)
		newer = [(remote_timestamp, node) for remote_timestamp, node in remote_timestamps if remote_timestamp > timestamp]
		logging.debug('newer: %s', newer)
		if newer:
			latest_timestamp, latest_node = newer[-1]
			latest_value = self.fetch_value(key, latest_node)
			if latest_value:
				value = latest_value
				timestamp = latest_timestamp
			self.store.set(key, value, timestamp)

		older = [(remote_timestamp, node) for remote_timestamp, node in remote_timestamps if remote_timestamp <	timestamp]
		logging.debug('older: %s', older)
		if older:
			older_nodes = [node for (remote_timestamp, node) in older]
			self.propagate(key, value, timestamp, older_nodes)

		return value, timestamp

def main():
	debug.configure_logging('nodeserver')

	parser = argparse.ArgumentParser(description="Hokanjo Node")
	parser.add_argument('-id','--id', help='Server id. Default = 1', type=int, default=1)
	parser.add_argument('-cfg','--config', help='Config file.', type=argparse.FileType('r'), default='etc/standalone.yaml')
	parser.add_argument('-f','--file', help='Database file. Default = data.tch', default='var/data/standalone.tch')

	try:
		args = parser.parse_args()
	except IOError, e:
		print >> sys.stderr, str(e)
		exit(-1)

	try:
		specification = yaml.load(args.config)
	except:
		print >> sys.stderr, 'Configuration file is not valid YAML.'
		exit(-1)

	configuration = Configuration()
	if not configuration.load(specification):
		print >> sys.stderr, 'Configuration is not valid.'
		exit(-1)

	if args.id not in configuration.active_deployment.nodes:
		print >> sys.stderr, 'Configuration for Node (id = %d) not found in configuration file (%s)' % (args.id, os.path.normpath(args.config.name))
		exit(-1)

	print 'Hokanjo Node'
	print '-' * 80
	print 'Node id: %d' % args.id
	print 'Config file: %s' % (args.config and args.config.name)
	print 'Serving up %s on port %d...' % (args.file, configuration.active_deployment.nodes[args.id].port)

	try:
		server = NodeServer(args.id, args.file, configuration)
		server.serve()
	except KeyboardInterrupt:
		pass

	print
	print 'Exiting...'

if __name__ == '__main__':
	import paths
	os.chdir(paths.home)
	main()