import gevent
from gevent.wsgi import WSGIServer
import urllib
import argparse
import yaml
import logging
import debug
import os, sys
from store import Store
from configuration import Configuration

def hashs(v):
	return str(hash(str(v)))

class NodeServer(object):
	def __init__(self, node_id, db_file, configuration):
		super(NodeServer, self).__init__()
		self.id = node_id
		self.file = db_file
		self.store = Store(self.file)
		self.store.open()
		self.configuration = configuration
		self.deployment = configuration.active_deployment
		self.node = self.deployment.nodes[self.id]
		self.siblings = self.deployment.siblings(self.id)
		self.wsgi_server = WSGIServer(('', self.node.port), self.handle_request)
		self.wsgi_server.serve_forever()

	def quote(self, key):
		"""docstring for quote"""
		return urllib.quote_plus(key, safe='/&')

	def unquote(self, path):
		"""docstring for unquote"""
		return urllib.unquote_plus(path)

	def store_GET(self, start_response, path):
		logging.debug("path: %s" % path)
		key = self.unquote(path)
		value = self.store.get(key)
		if value:
			start_response('200 OK', [('Content-Type', 'application/octet-stream')])
			return value
		else:
			start_response('404 Not Found', [('Content-Type', 'application/octet-stream')])
			return ''

	def store_POST(self, start_response, path, body):
		logging.debug("path: %s" % path)
		key = self.unquote(path)
		value = body.read()
		self.store.set(key, value)
		self.propagate(key, value)
		start_response('200 OK', [('Content-Type', 'application/octet-stream')])
		return ''

	def internal_GET(self, start_response, path):
		"""docstring for internal_GET"""
		logging.debug("path: %s" % path)
		key = self.unquote(path)
		value = self.store.get(key)
		if value:
			start_response('200 OK', [('Content-Type', 'application/octet-stream')])
			return value
		else:
			start_response('404 Not Found', [('Content-Type', 'application/octet-stream')])
			return ''

	def internal_POST(self, start_response, path, body):
		"""docstring for internal_POST"""
		logging.debug("path: %s" % path)
		key = self.unquote(path)
		value = body.read()
		self.store.set(key, value)
		start_response('200 OK', [('Content-Type', 'application/octet-stream')])
		return ''

	def send(self, key, value, node):
		logging.debug('Posting %s to sibling (%s)' % (repr(key), node))
		path = self.quote(key)
		try:
			url = node.internal_url() + path
			stream = urllib.urlopen(url, value)
			stream.read()
			stream.close()
		except IOError, e:
			logging.error('Failed to post data to sibling (%s): %s' % (node, e))

	def propagate(self, key, value):
		"""docstring for propagate"""
		neighbour_buckets = self.configuration.find_neighbour_buckets(key, self.node)
		neighbour_bucket_nodes = [node for bucket in neighbour_buckets for node in bucket]
		target_nodes = self.siblings + neighbour_bucket_nodes
		greenlets = [gevent.spawn(self.send, key, value, node) for node in target_nodes]
		logging.debug('target nodes: %s' % repr(target_nodes))
		gevent.joinall(greenlets)

	def handle_request(self, env, start_response):
		method = env['REQUEST_METHOD']
		path = env['PATH_INFO']
		body = env['wsgi.input']
		handlers = (
			('/store/', (self.store_GET, self.store_POST)),
			('/internal/', (self.internal_GET, self.internal_POST)),
		)
		for prefix, (get, post) in handlers:
			if path.startswith(prefix):
				sub_path = path[len(prefix):]
				if method == 'GET':
					return get(start_response, sub_path)
				elif method == 'POST':
					return post(start_response, sub_path, body)
		start_response('404 Not Found', [])
		return ''

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