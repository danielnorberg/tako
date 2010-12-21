import argparse
from utils import debug
import os, sys
import httpserver
import simplejson as json
import configuration

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

class CoordinatorServer(httpserver.HttpServer):
	def __init__(self, coordinator_id, configuration, configuration_filepath):
		super(CoordinatorServer, self).__init__()
		self.id = coordinator_id
		self.original_configuration = configuration
		self.configuration = Configuration(configuration.specification())
		self.coordinator = configuration.coordinators[self.id]
		self.configuration_filepath = configuration_filepath
		self.handlers = (
			('/configuration', {'GET': self.configuration_GET}),
		)
		self.port = self.coordinator.port

	def reload_configuration(self):
		"""docstring for reload_configuration"""
		pass

	def configuration_GET(self, start_response, path, body, env):
		"""docstring for configuration_GET"""
		start_response('200 OK', [
			('Content-Type', 'application/json'),
			('X-TimeStamp', str(self.configuration.timestamp)),
		])
		return [json.dumps(self.configuration.specification())]

def main():
	debug.configure_logging('coordinatorserver')

	parser = argparse.ArgumentParser(description="Tako Coordinator")
	parser.add_argument('-id', '--id', help='Server id. Default = 1', default='c1')
	parser.add_argument('-cfg','--config', help='Config file.', default='test/local_cluster.yaml')

	try:
		args = parser.parse_args()
	except IOError, e:
		print >> sys.stderr, str(e)
		exit(-1)

	cfg = configuration.try_load_file(args.config)

	if not cfg:
		print >> sys.stderr, 'Failed to load configuration.'
		exit(-1)

	print 'Tako Coordinator'
	print '-' * 80
	print 'Coordinator id: %s' % args.id
	print 'Config file: %s' % (args.config)
	print 'Serving up %s on port %d' % (args.config, cfg.coordinators[args.id].port)

	try:
		server = CoordinatorServer(args.id, cfg, args.config)
		server.serve()
	except KeyboardInterrupt:
		pass

	print
	print 'Exiting...'

if __name__ == '__main__':
	import paths
	os.chdir(paths.home)
	main()