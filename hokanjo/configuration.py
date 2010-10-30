from consistenthash import ConsistentHash

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
		hash_configuration = 'hash' in specification and specification['hash'] or {}
		self.consistent_hash = ConsistentHash(self.buckets.values(), **hash_configuration)

	def siblings(self, node_id):
		"""docstring for siblings"""
		node = self.nodes[node_id]
		bucket = self.buckets[node.bucket_id]
		siblings = dict(bucket.nodes)
		del siblings[node_id]
		return siblings.values()

	def specification(self):
		return {
			'hash': {
				'buckets_per_key': self.consistent_hash.buckets_per_key,
			},
			'buckets':dict(
				(bucket.id, [[node.id, node.address, node.port] for node in bucket]) for bucket in self.buckets.values()
			),
		}


class Configuration(object):
	"""docstring for Configuration"""
	def __init__(self, specification=None):
		super(Configuration, self).__init__()
		if specification:
			self.load(specification)

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)

	def __str__(self):
		"""docstring for fname"""
		return "Configuration(Buckets=%s)" % ', '.join(str(bucket) for bucket in self.buckets.values())

	def load(self, specification):
		"""docstring for load"""
		if not self.validate_specification(specification):
			return False
		self.original_specification = specification
		self.deployments = dict((name, Deployment(name, deployment_specification)) for name, deployment_specification in specification['deployments'].iteritems())
		self.active_deployment_name = specification['active_deployment']
		self.active_deployment = self.deployments[self.active_deployment_name]
		return True

	def specification(self):
		"""docstring for yaml"""
		return {
			"active_deployment": self.active_deployment_name,
			"deployments": dict((deployment.name, deployment.specification()) for deployment in self.deployments.itervalues())
		}

	def validate_specification(self, specification):
		"""docstring for validate_specification"""
		valid =	'active_deployment' in specification and \
				'deployments' in specification and \
				len(specification['deployments']) > 0 and \
				specification['active_deployment'] in specification['deployments']
		return valid

	def find_neighbour_buckets(self, key, node):
		"""docstring for find_neighbour_buckets"""
		node_bucket = self.active_deployment.buckets[node.bucket_id]
		key_buckets = self.active_deployment.consistent_hash.find_buckets(key)
		return key_buckets - set([node_bucket])

if __name__ == '__main__':
	import yaml
	import paths
	configuration = Configuration(yaml.load(open(paths.path('config.yaml'))))
	print yaml.dump(configuration.specification())
	print configuration.active_deployment.consistent_hash
