import os, logging

from utils import testcase
from utils.timestamp import Timestamp
import configuration
import paths

MAX_CONFIGURATION_HISTORY = 10

class ConfigurationCache(object):
	"""docstring for ConfigurationCache"""
	def __init__(self, directory, name):
		super(ConfigurationCache, self).__init__()
		self.directory = directory
		self.name = name

	def list_files(self):
		try:
			return sorted([filename for filename in os.listdir(self.directory) if os.path.splitext(filename)[1] == '.yaml' and filename.startswith(self.name)])
		except OSError, e:
			logging.debug(e)
			return []

	def cleanup(self):
		filenames = self.list_files()
		if len(filenames) > MAX_CONFIGURATION_HISTORY:
			remove_count = len(filenames) - MAX_CONFIGURATION_HISTORY
			remove_files = filenames[0:remove_count]
			logging.debug('Removing files: %s' % ', '.join([repr(filename) for filename in remove_files]))
			for filename in remove_files:
				filepath = os.path.join(self.directory, filename)
				try:
					os.unlink(filepath)
				except OSError, e:
					logging.error('Failed to remove configuration file: %s', e)

	def get_configuration(self):
		filenames = self.list_files()
		filenames.reverse()
		for filename in filenames:
			name, ext = os.path.splitext(filename)
			name_parts = name.split('.')
			timestamp_string = name_parts[-1]
			timestamp = Timestamp.loads(timestamp_string)
			filepath = os.path.join(self.directory, filename)
			persisted_configuration = configuration.try_load_file(filepath, timestamp)
			if persisted_configuration:
				return persisted_configuration
		return None

	def cache_configuration(self, cfg):
		filename = '%s.%s.yaml' % (self.name, cfg.timestamp)
		filepath = os.path.join(self.directory, filename)
		if configuration.try_dump_file(filepath, cfg):
			self.cleanup()
			return True
		return False

class TestConfiguration(testcase.TestCase):
	def testPersistence(self):
		"""docstring for testPersistence"""
		files = ['test/config.yaml', 'test/local_cluster.yaml', 'test/migration.yaml']
		for f in files:
			configuration_directory = self.tempdir()
			cache = ConfigurationCache(configuration_directory, 'test')
			filepath = paths.path(f)
			cfg = configuration.try_load_file(filepath)
			for i in xrange(0, 100):
				cfg.timestamp = Timestamp.now()
				cache.cache_configuration(cfg)
				read_configuration = cache.get_configuration()
				self.assertEqual(read_configuration.specification(), cfg.specification())
				self.assertEqual(read_configuration.timestamp, cfg.timestamp)

if __name__ == '__main__':
	import unittest
	unittest.main()