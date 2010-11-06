import pprint
import yaml
import simplejson as json
import logging
import os

from utils import testcase
from utils.timestamp import Timestamp
from models import Coordinator, Deployment

def try_load_specification(specification, timestamp=Timestamp.now()):
	"""docstring for try_load_specification"""
	try:
		return Configuration(specification, timestamp)
	except ValidationError, e:
		logging.error('Configuration is not valid: %s', e)
		return None

def try_load_json(s, timestamp=Timestamp.now()):
	try:
		specification = json.loads(s)
	except Exception, e:
		logging.error('Failed to parse JSON configuration: %s', e)
		return None
	return try_load_specification(specification, timestamp)

def try_load_file(filepath, timestamp=None):
	"""docstring for load"""
	try:
		if not timestamp:
			timestamp = Timestamp.from_seconds(os.path.getmtime(filepath))
		with open(filepath) as f:
			specification = yaml.load(f)
	except OSError, e:
		logging.error('Failed reading configuration file: %s', e)
		return None
	except IOError, e:
		logging.error('Failed reading configuration file: %s', e)
		return None
	except Exception, e:
		logging.error('Failed reading configuration file: %s', e)
		return None
	return try_load_specification(specification, timestamp)

TIME_FORMAT = '%Y%m%d%H%M%S-%%06d'

def read_persisted_configuration(configuration_directory, prefix):
	"""docstring for read_persisted_configuration"""
	try:
		filenames = [filename for filename in os.listdir(configuration_directory) if os.path.splitext(filename)[1] == '.yaml' and filename.startswith(name)]
	except OSError, e:
		logging.debug(e)
		return None
	filenames.sort(reverse=True)
	for filename in filenames:
		name, ext = os.path.splitext(filename)
		name_parts = name.split('')
		timestamp_string = name_parts[-1]
		timestamp = Timestamp.loads(timestamp_string)
		filepath = os.path.join(self.configuration_directory, filename)
		persisted_configuration = configuration.try_load_file(filepath, timestamp)
		if persisted_configuration:
			return persisted_configuration
	return None

def persist_configuration(self, prefix):
	"""docstring for persist_configuration"""
	filename = '%s.%s.yaml' % (prefix, self.configuration.timestamp)
	filepath = os.path.join(self.configuration_directory, filename)
	configuration.try_dump_file(self.configuration)

def validate_specification(specification):
	"""docstring for validate_specification"""
	# TODO: Complete validation
	try:
		assert 'active_deployment' in specification
		assert 'deployments' in specification
		assert len(specification['deployments']) > 0
		assert specification['active_deployment'] in specification['deployments']
		if 'target_deployment' in specification:
			assert specification['target_deployment'] in specification['deployments']
		for deployment_id, deployment in specification['deployments'].iteritems():
			assert 'buckets' in deployment
			for bucket_id, bucket in deployment['buckets'].iteritems():
				assert len(bucket) > 0
				for node_id, node in bucket.iteritems():
					assert len(node) == 2
					address, port = node
					assert type(address) == str
					assert type(port) == int
		if 'master_coordinator' in specification:
			assert specification['master_coordinator'] in specification['coordinators']
		for coordinator_id, coordinator in specification.get('coordinators', {}).iteritems():
			assert len(coordinator) == 2
			address, port = coordinator
			assert type(address) == str
			assert type(port) == int
	except AssertionError:
		raise ValidationError()

class ValidationError(object):
	"""docstring for ValidationError"""
	def __init__(self, description=None):
		super(ValidationError, self).__init__()
		self.description = description

	def __str__(self):
		"""docstring for __str__"""
		return 'ValidationError(%s)' % (self.description or '')

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)

class Configuration(object):
	"""docstring for Configuration"""
	def __init__(self, specification=None, timestamp=Timestamp.now()):
		super(Configuration, self).__init__()
		self.timestamp = timestamp
		if specification:
			assert self.load(specification)

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)

	def __str__(self):
		"""docstring for fname"""
		return "Configuration(%s)" % self.specification()

	def load(self, specification):
		"""docstring for load"""
		validate_specification(specification)
		self.original_specification = specification
		self.deployments = dict((name, Deployment(name, deployment_specification)) for name, deployment_specification in specification.get('deployments', {}).iteritems())
		self.active_deployment_name = specification['active_deployment']
		self.active_deployment = self.deployments[self.active_deployment_name]
		self.target_deployment_name = specification.get('target_deployment', None)
		self.target_deployment = self.target_deployment_name and self.deployments[self.target_deployment_name]
		self.coordinators = dict((coordinator_id, Coordinator(coordinator_id, address, port)) for coordinator_id, (address, port) in specification.get('coordinators', {}).iteritems())
		self.master_coordinator_id = specification.get('master_coordinator', None)
		self.master_coordinator = self.coordinators.get(self.master_coordinator_id, None)
		return True

	def specification(self):
		"""docstring for yaml"""
		spec = {
			'active_deployment': self.active_deployment_name,
			'deployments': dict((deployment.name, deployment.specification()) for deployment in self.deployments.itervalues()),
		}
		if self.coordinators:
			spec['coordinators'] = dict((coordinator.id, [coordinator.address, coordinator.port]) for coordinator in self.coordinators.itervalues())
		if self.master_coordinator:
			spec['master_coordinator'] = self.master_coordinator_id
		if self.target_deployment_name:
			spec['target_deployment'] = self.target_deployment_name
		return spec

	def find_neighbour_buckets(self, key, node):
		"""docstring for find_neighbour_buckets"""
		node_bucket = self.active_deployment.buckets[node.bucket_id]
		key_buckets = self.active_deployment.consistent_hash.find_buckets(key)
		return key_buckets - set([node_bucket])


class TestConfiguration(testcase.TestCase):
	def testParsing(self):
		import yaml
		import paths
		files = ['test/config.yaml', 'test/local_cluster.yaml', 'test/migration.yaml']
		for f in files:
			print
			pp = pprint.PrettyPrinter()
			filepath = paths.path(f)
			with open(filepath) as specfile:
				loaded_specification = yaml.load(specfile)
				pp.pprint(loaded_specification)
				timestamp = Timestamp.from_seconds(os.path.getmtime(filepath))
				helper_loaded_configuration = try_load_file(filepath)
				manually_loaded_configuration = Configuration(loaded_specification, timestamp)
				self.assertEqual(manually_loaded_configuration.specification(), loaded_specification)
				self.assertEqual(manually_loaded_configuration.specification(), helper_loaded_configuration.specification())
				self.assertEqual(manually_loaded_configuration.timestamp, helper_loaded_configuration.timestamp)

if __name__ == '__main__':
	import unittest
	unittest.main()