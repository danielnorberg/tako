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
		return "Node(id=%s, bucket=%s, %s:%d)" % (self.id, self.bucket_id, self.address, self.port)

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
		return "Coordinator(id=%s, %s:%d)" % (self.id, self.address, self.port)

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)

	def configuration_url(self):
		"""docstring for configuration_url"""
		return 'http://%s:%d/configuration' % (self.address, self.port)
