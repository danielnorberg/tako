import logging
import gevent
import configuration
import unittest
import testcase
import paths
from utils import http, convert

class CoordinatorClient(object):
	"""docstring for CoordinatorClient"""
	def __init__(self, coordinators=[], callbacks=None, interval=30):
		super(CoordinatorClient, self).__init__()
		self.coordinators = coordinators
		self.callbacks = callbacks
		self.interval = interval
		self.greenlet = None
		self.timestamp = 0.0

	def start(self):
		"""docstring for start"""
		self.greenlet = gevent.spawn(self.fetch_configurations)

	def broadcast(self):
		"""docstring for broadcast"""
		for f in self.callbacks:
			f(self.configuration, self.timestamp)

	def fetch_configuration(self, coordinator):
		logging.debug('coordinator: %s', coordinator)
		url = coordinator.configuration_url()
		body, info = http.fetch(url)
		if body:
			logging.debug('Got specification: %s', body)
			new_configuration = configuration.try_load_json(body)
			new_timestamp = convert.try_float(info.get('X-TimeStamp', None))
			return (new_timestamp, new_configuration)
		else:
			return (None, None)

	def fetch_configurations(self):
		"""docstring for fetch_configuration"""
		while True:
			logging.debug('coordinators: %s', self.coordinators)
			if self.coordinators:
				greenlets = [gevent.spawn(self.fetch_configuration, coordinator) for coordinator in self.coordinators]
				gevent.joinall(greenlets)
				configurations = sorted(greenlet.value for greenlet in greenlets)
				logging.debug('configurations: %s', configurations)
				for new_timestamp, new_configuration in configurations:
					if new_timestamp and new_configuration and new_timestamp > self.timestamp:
						self.configuration = new_configuration
						self.timestamp = new_timestamp
						self.source_coordinator = coordinator
						self.broadcast()
						break
			gevent.sleep(self.interval)

class TestCoordinatorClient(testcase.TestCase):
	def callback(self, new_timestamp, new_configuration):
		logging.debug('new_timestamp: %s, new_configuration: %s', repr(new_timestamp), new_configuration)
		self.new_configuration=new_configuration
		self.new_timestamp=new_timestamp

	def testClient(self):
		"""docstring for testClient"""
		from coordinatorserver import CoordinatorServer
		cfg_filepath = 'test/local_cluster.yaml'
		cfg = configuration.try_load_file(paths.path(cfg_filepath))
		coordinator_server = CoordinatorServer(cfg.master_coordinator_id, cfg, cfg_filepath)
		coordinator_greenlet = gevent.spawn(coordinator_server.serve)
		gevent.sleep(0)
		self.new_configuration = None
		self.new_timestamp = None
		client = CoordinatorClient(coordinators=[cfg.master_coordinator], callbacks=[self.callback])
		client.start()
		for i in xrange(0, 1000):
			gevent.sleep(0)
			if self.new_configuration or self.new_timestamp:
				break
		print self.new_configuration
		print self.new_timestamp
		coordinator_greenlet.kill()


if __name__ == '__main__':
	unittest.main()