import logging
import unittest

from syncless import coio

from syncless import patch
patch.patch_socket()

from utils import runner
from utils import testcase
from utils.timestamp import Timestamp
from utils import http

import configuration
import paths
from coordinatorserver import CoordinatorServer

class CoordinatorClient(object):
	"""docstring for CoordinatorClient"""
	def __init__(self, coordinators=[], callbacks=None, interval=30):
		super(CoordinatorClient, self).__init__()
		self.coordinators = coordinators
		self.callbacks = callbacks
		self.interval = interval
		self.configuration_fetcher = None
		self.configuration = None

	def start(self):
		"""docstring for start"""
		self.configuration_fetcher = runner.spawn(self.fetch_configurations)

	def broadcast(self):
		"""docstring for broadcast"""
		logging.debug('configuration: %s', self.configuration)
		for f in self.callbacks:
			f(self.configuration)

	def fetch_configuration(self, coordinator):
		# logging.debug('coordinator: %s', coordinator)
		url = coordinator.configuration_url()
		body, info = http.fetch(url)
		if body:
			# logging.debug('Got specification: %s', body)
			new_timestamp = Timestamp.try_loads(info.get('X-TimeStamp', None))
			if new_timestamp:
				new_configuration = configuration.try_load_json(body, timestamp=new_timestamp)
				return (new_configuration, coordinator)
		return (None, coordinator)

	def set_configuration(self, new_configuration):
		"""docstring for reconfigure"""
		self.configuration = new_configuration
		self.coordinators = new_configuration.coordinators.values()

	def fetch_configurations(self):
		"""docstring for fetch_configuration"""
		while True:
			# logging.debug('coordinators: %s', self.coordinators)
			if self.coordinators:
				configurations = runner.run([runner.task(self.fetch_configuration, coordinator) for coordinator in self.coordinators])
				configurations.sort()
				for new_configuration, source_coordinator in configurations:
					if new_configuration and (not self.configuration or new_configuration.timestamp > self.configuration.timestamp):
						self.set_configuration(new_configuration)
						self.broadcast()
						break
			coio.sleep(self.interval)

class TestCoordinatorClient(testcase.TestCase):
	def callback(self, new_configuration):
		logging.debug('timestamp: %s, new_configuration: %s', new_configuration.timestamp, new_configuration)
		self.new_configuration=new_configuration

	def testClient(self):
		"""docstring for testClient"""
		cfg_filepath = 'test/local_cluster.yaml'
		cfg = configuration.try_load_file(paths.path(cfg_filepath))
		coordinator_server = CoordinatorServer(cfg.master_coordinator_id, cfg, cfg_filepath)
		coordinator_server_task = runner.spawn(coordinator_server.serve)
		coio.stackless.schedule()
		self.new_configuration = None
		self.new_timestamp = None
		client = CoordinatorClient(coordinators=[cfg.master_coordinator], callbacks=[self.callback])
		client.start()
		for i in xrange(0, 1000):
			coio.stackless.schedule()
			if self.new_configuration or self.new_timestamp:
				break
		assert cfg.specification() == self.new_configuration.specification()
		print 'Fetched configuration: ', self.new_configuration
		print 'Timestamp', self.new_timestamp
		coordinator_server_task.kill()


if __name__ == '__main__':
	unittest.main()