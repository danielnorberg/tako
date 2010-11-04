import gevent, gevent.monkey
gevent.monkey.patch_all()
import urllib, urllib2
import argparse
import logging
import debug
import os, sys
import email.utils
import time
import httpserver
import paths

from utils import convert
from utils import http

from store import Store
import configuration
from configuration import Coordinator

class NodeServer(httpserver.HttpServer):
	def __init__(self, node_id, store_file=None, cfg=None, coordinators=[], var_directory='var'):
		super(NodeServer, self).__init__()
		self.id = node_id
		self.var_directory = os.path.join(paths.home, var_directory)
		self.store_file = store_file or os.path.join(self.var_directory, 'data', '%s.tch' % self.id)
		self.configuration_directory = os.path.join(self.var_directory, 'etc', str(node_id))
		self.store = Store(self.store_file)
		self.store.open()
		self.handlers = (
			('/store/', {'GET':self.store_GET, 'POST':self.store_POST}),
			('/internal/', {'GET':self.internal_GET, 'POST':self.internal_POST}),
			('/stat/', {'GET':self.stat_GET}),
		)

		if not cfg:
			cfg = self.read_persisted_configuration()

		if not cfg and coordinators:
			cfg = self.fetch_configuration(coordinators, tries=0)

		if not cfg:
			raise Exception("No configuration.")

		self.configure(cfg)

	def configure(self, cfg):
		"""docstring for configuration"""
		logging.debug(cfg)
		self.configuration = cfg
		self.deployment = self.configuration.active_deployment
		self.read_repair_enabled = self.configuration.active_deployment.read_repair_enabled
		self.node = self.deployment.nodes[self.id]
		self.siblings = self.deployment.siblings(self.id)
		self.port = self.node.port

	def read_persisted_configuration(self):
		"""docstring for read_persisted_configuration"""
		try:
			filenames = [name for name in os.listdir(self.configuration_directory) if os.path.splitext(name)[1] == '.yaml']
		except OSError, e:
			logging.debug(e)
			return None
		filenames.sort(reverse=True)
		for filename in filenames:
			filepath = os.path.join(self.configuration_directory, filename)
			cfg = configuration.load(filepath)
			if cfg:
				return cfg
		return None

	def fetch_configuration(self, coordinators, tries=3, max_backoff=60*60):
		"""docstring for fetch_configuration"""
		logging.debug('coordinators: %s', coordinators)
		backoff = 3
		tries = tries or sys.maxint
		for i in xrange(0, tries):
			logging.info('Attempt %d', i)
			for coordinator in coordinators:
				url = coordinator.configuration_url()
				body, info = http.fetch(url)
				if body:
					logging.debug('Got specification: %s', body)
					cfg = configuration.try_load_json(body)
					if cfg:
						return cfg
					else:
						logging.error('Got corrupt configuration from coordinator %s', repr(url))
			logging.info('Sleeping for %d seconds', backoff)
			time.sleep(backoff)
			backoff = min(max_backoff, backoff * 2)
		logging.error('Failed to fetch configuration.')
		return None

	def serve(self):
		"""docstring for server"""
		if not self.configuration:
			raise Exception("Not configured.")
		super(NodeServer, self).serve()

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
		url = node.internal_url() + path
		body, info = http.fetch(url)
		return body

	def fetch_timestamp(self, key, node):
		"""docstring for fetch_timestamp"""
		logging.debug('key: %s, node: %s', repr(key), node)
		path = self.quote(key)
		url = node.stat_url() + path
		body, info = http.fetch(url)
		timestamp = convert.try_float(info.get('X-TimeStamp', None))
		logging.debug('key: %s, node: %s, timestamp: %s', repr(key), node, repr(timestamp))
		return (timestamp, node)

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
			raise httpserver.BadRequest()

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
	parser.add_argument('-id', '--id', help='Server id. Default = n1', default='n1')
	parser.add_argument('-c', '--coordinator', help='Coordinator Server (address port)', nargs=2, action='append')
	parser.add_argument('-f','--file', help='Database file.')
	parser.add_argument('-cfg','--config', help='Configuration file. For use without a coordinator.')

	try:
		args = parser.parse_args()
	except IOError, e:
		print >> sys.stderr, str(e)
		exit(-1)

	config = None
	if args.config:
		config = configuration.load(args.config)
		if not config:
			print >> sys.stderr, 'Failed to load configuration file.'
			exit(-1)
		if args.id not in configuration.active_deployment.nodes:
			print >> sys.stderr, 'Configuration for Node (id = %s) not found in configuration' % args.id
			exit(-1)

	print 'Hokanjo Node'
	print '-' * 80
	print 'Node id: %s' % args.id

	coordinators = []
	for address, port_string in args.coordinator:
		port = convert.try_int(port_string)
		if not port:
			print >> sys.stderr, 'Port number is not numerical.'
			exit(-1)
		coordinators.append(Coordinator(None, address, port))

	try:
		server = NodeServer(args.id, store_file=args.file, cfg=config, coordinators=coordinators)
		server.serve()
	except KeyboardInterrupt:
		pass

	print
	print 'Exiting...'

if __name__ == '__main__':
	os.chdir(paths.home)
	main()