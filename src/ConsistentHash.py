import hashlib

class ConsistentHash(object):
	"""Implements a consistent hash
	http://www.tomkleinpeter.com/2008/03/17/programmers-toolbox-part-3-consistent-hashing/
	"""
	def __init__(self, points_per_bucket = 17, buckets_per_key = 3):
		super(ConsistentHash, self).__init__()
		self.points_per_bucket = points_per_bucket
		self.buckets = []
		self.points = []
		self.buckets_per_key = buckets_per_key

	def add_bucket(self, bucket):
		"""docstring for add_bucket"""
		self.buckets.append(bucket)
		for i in xrange(1, self.points_per_bucket):
			point = self.generate_point("%s-%s" % (bucket, i))
			self.points.append((point, bucket))
		self.points.sort()

	def add_buckets(self, buckets):
		"""docstring for add_buckets"""
		for bucket in buckets:
			self.add_bucket(bucket)

	def remove_bucket(self):
		"""docstring for remove_bucket"""
		pass

	def remove_buckets(self, buckets):
		"""docstring for remove_buckets"""
		pass

	def find_point(self, key):
		"""docstring for find_point"""
		key_point = self.generate_point(key)
		for	i, (bucket_point, bucket) in enumerate(self.points):
			if bucket_point > key_point:
				return i, (bucket_point, bucket)
		return 0, self.points[0]

	def buckets_from_point(self, i):
		"""docstring for points_from"""
		while True:
			point, bucket = self.points[i % len(self.points)]
			yield i, bucket
			i += 1

	def find_buckets(self, key):
		"""docstring for find_buckets"""
		buckets = set()
		i, (key_point, key_bucket) = self.find_point(key)
		for	j, bucket in self.buckets_from_point(key_point):
			buckets.add(bucket)
			if len(buckets) >= self.buckets_per_key or len(buckets) >= len(self.buckets):
				break
		return buckets

	def generate_point(self, key):
		"""docstring for generate_point"""
		key_hash = hashlib.sha256()
		key_hash.update(key)
		return int(key_hash.hexdigest()[:8], 16)

	def migrate(self, keys, target):
		"""docstring for migrate"""
		migration_mapping = {}
		for	key in keys:
			source_buckets = self.find_buckets(key)
			target_buckets = target.find_buckets(key)
			removed_buckets = source_buckets.difference(target_buckets)
			added_buckets = target_buckets.difference(source_buckets)
			if removed_buckets or added_buckets:
				migration_mapping[key] = (removed_buckets, added_buckets)
		return migration_mapping


def testBucketing():
	ch = ConsistentHash(buckets_per_key = 5)
	servers = [str(i) for i in xrange(0, 7)]
	ch.add_buckets(servers)
	keys = range(0, 100)
	key_servers = dict([(key, set()) for key in keys])
	for	key in keys:
		servers_for_key = ch.find_buckets(str(key))
		for server in servers_for_key:
			key_servers[key].add(server)
		assert(len(servers_for_key) == 5)

def testMigration():
	"""docstring for testMigration"""
	s1 = [str(i) for i in xrange(0, 9)]
	s2 = [str(i) for i in xrange(0, 15)]
	keys = [str(i) for i in xrange(0, 17)]
	buckets_per_key_delta = 2
	ch1 = ConsistentHash(buckets_per_key=3)
	ch1.add_buckets(s1)
	ch2 = ConsistentHash(buckets_per_key=3 + buckets_per_key_delta)
	ch2.add_buckets(s2)
	migration_mapping = ch1.migrate(keys, ch2)
	for key, (source_servers, target_servers) in migration_mapping.iteritems():
		# print key, ': ', sorted(source_servers), '=>', sorted(target_servers)
		assert(len(target_servers) - len(source_servers) >= buckets_per_key_delta)


if __name__ == "__main__":
	testBucketing()
	testMigration()