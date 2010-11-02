from consistenthash import ConsistentHash
import testcase
import pprint

class Node(object):
	"""docstring for Node"""
	def __init__(self, node_id, bucket_id, address, port):
		super(Node, self).__init__()
		self.id = node_id
		self.bucket_id = bucket_id
		self.address = address
		self.port = port

	def __str__(self):
		"""docstring for __str__"""
		return "Node(id=%d, bucket=%d, %s:%d)" % (self.id, self.bucket_id, self.address, self.port)

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)

	def internal_url(self):
		"""docstring for url"""
		return 'http://%s:%d/internal/' % (self.address, self.port)

	def store_url(self):
		"""docstring for store_url"""
		return 'http://%s:%d/store/' % (self.address, self.port)

	def stat_url(self):
		"""docstring for stat_url"""
		return 'http://%s:%d/stat/' % (self.address, self.port)

class Bucket(object):
	"""docstring for Bucket"""
	def __init__(self, bucket_id, nodes):
		super(Bucket, self).__init__()
		self.id = bucket_id
		self.nodes = nodes

	def __str__(self):
		return 'Bucket(%s)' % ', '.join([str(node) for node in self.nodes.itervalues()])

	def __repr__(self):
		return str(self)

	def __iter__(self):
		return self.nodes.itervalues()

	def __hash__(self):
		return hash(self.id)



class Deployment(object):
	"""docstring for Deployment"""
	def __init__(self, name, specification):
		super(Deployment, self).__init__()
		self.original_specification = specification
		self.name = name
		self.buckets = dict((bucket_id, Bucket(bucket_id, dict((node_id, Node(node_id, bucket_id, address, port)) for node_id, (address, port) in bucket.iteritems()))) \
							for bucket_id, bucket in specification['buckets'].iteritems())
		self.nodes = dict((node_id, node) for bucket in self.buckets.itervalues() for node_id, node in bucket.nodes.iteritems())
		hash_configuration = specification.get('hash', {})
		self.consistent_hash = ConsistentHash(self.buckets.values(), **hash_configuration)
		self.read_repair_enabled = specification.get('read_repair', True)

	def siblings(self, node_id):
		"""docstring for siblings"""
		node = self.nodes[node_id]
		bucket = self.buckets[node.bucket_id]
		siblings = dict(bucket.nodes)
		del siblings[node_id]
		return siblings.values()

	def specification(self):
		spec = {
			'read_repair': self.read_repair_enabled,
			'hash': {
				'buckets_per_key': self.consistent_hash.buckets_per_key,
			},
			'buckets':dict((bucket.id, dict((node.id, [node.address, node.port]) for node in bucket)) for bucket in self.buckets.values()),
		}
		return spec

	def __str__(self):
		"""docstring for __str__"""
		return 'Deployment(read_repair=%s, hash=%s, buckets=%s)' % (self.read_repair_enabled, {'buckets_per_key':self.consistent_hash.buckets_per_key}, ', '.join([str(bucket) for bucket in self.buckets.itervalues()]))

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)

class Coordinator(object):
	"""docstring for Coordinator"""
	def __init__(self, coordinator_id, address, port):
		super(Coordinator, self).__init__()
		self.id = coordinator_id
		self.address = address
		self.port = port

	def __str__(self):
		"""docstring for __str__"""
		return "Coordinator(id=%d, %s:%d)" % (self.id, self.address, self.port)

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)


class Configuration(object):
	"""docstring for Configuration"""
	def __init__(self, specification=None):
		super(Configuration, self).__init__()
		if specification:
			assert self.load(specification)

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)

	def __str__(self):
		"""docstring for fname"""
		return "Configuration(Buckets=%s)" % ', '.join(str(bucket) for bucket in self.buckets.values())

	def load(self, specification):
		"""docstring for load"""
		self.validate_specification(specification)
		self.original_specification = specification
		self.deployments = dict((name, Deployment(name, deployment_specification)) for name, deployment_specification in specification.get('deployments', {}).iteritems())
		self.active_deployment_name = specification['active_deployment']
		self.active_deployment = self.deployments[self.active_deployment_name]
		self.target_deployment_name = specification.get('target_deployment', None)
		self.target_deployment = self.target_deployment_name and self.deployments[self.target_deployment_name]
		self.coordinators = dict((coordinator_id, Coordinator(coordinator_id, address, port)) for coordinator_id, (address, port) in specification.get('coordinators', {}).iteritems())
		self.master_coordinator = specification.get('master_coordinator', None)
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
			spec['master_coordinator'] = self.master_coordinator
		if self.target_deployment_name:
			spec['target_deployment'] = self.target_deployment_name
		return spec

	def validate_specification(self, specification):
		"""docstring for validate_specification"""
		# TODO: Complete validation
		assert 'active_deployment' in specification
		assert 'deployments' in specification
		assert len(specification['deployments']) > 0
		assert specification['active_deployment'] in specification['deployments']
		if 'target_deployment' in specification:
			assert specification['target_deployment'] in specification['deployments']

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
			loaded_specification = yaml.load(open(paths.path(f)))
			pp.pprint(loaded_specification)
			configuration = Configuration(loaded_specification)
			generated_specification = configuration.specification()
			pp.pprint(generated_specification)
			self.assertEqual(generated_specification, loaded_specification)

if __name__ == '__main__':
	import unittest
	unittest.main()