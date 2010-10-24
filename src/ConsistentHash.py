import hashlib
import bisect

class ConsistentHash(object):
	"""Implements a consistent hash"""
	def __init__(self, points_per_bucket = 143, buckets_per_key = 3):
		super(ConsistentHash, self).__init__()
		self.points_per_bucket = points_per_bucket
		self.buckets = []
		self.points = []
		self.buckets_per_key = buckets_per_key

	def _add_bucket(self, bucket):
		"""docstring for _add_bucket"""
		self.buckets.append(bucket)
		for i in xrange(1, self.points_per_bucket):
			point = self.generate_point("%s-%s" % (bucket, i))
			self.points.append((point, bucket))

	def _update_point_index(self):
		"""docstring for _update_point_index"""
		self.points.sort()
		self.point_index = [point[0] for point in self.points]

	def add_bucket(self, bucket):
		self._add_bucket(bucket)
		self._update_point_index()

	def add_buckets(self, buckets):
		"""docstring for add_buckets"""
		for bucket in buckets:
			self._add_bucket(bucket)
		self._update_point_index()

	def find_point(self, key):
		"""docstring for find_point"""
		key_point = self.generate_point(key)
		i = bisect.bisect(self.point_index, key_point) % len(self.points)
		return i, self.points[i]

	def points_from(self, index, reverse=False):
		"""docstring for points_from"""
		delta = -1 if reverse else 1
		len_points = len(self.points)
		while True:
			index += delta
			i = index % len_points
			point, bucket = self.points[i]
			yield i, point, bucket

	def find_buckets(self, key):
		"""docstring for find_buckets"""
		buckets = set()
		i, (key_point, key_bucket) = self.find_point(key)
		buckets.add(key_bucket)
		for	j, point, bucket in self.points_from(i):
			if len(buckets) >= self.buckets_per_key or len(buckets) >= len(self.buckets):
				break
			buckets.add(bucket)
		return buckets

	def generate_point(self, key):
		"""docstring for generate_point"""
		# Using md5 as it is slightly faster than sha
		key_hash = hashlib.md5()
		key_hash.update(key)
		return int(key_hash.hexdigest()[:8], 16)

	def generate_points(self, keys):
		"""docstring for generate_points"""
		key_hash = hashlib.sha256()
		for key in keys:
			key_hash.update(key)
			yield int(key_hash.hexdigest()[:8], 16)

	def key_migration_mapping(self, keys, target):
		"""docstring for key_migration_mapping"""
		migration_mapping = {}
		for	key in keys:
			source_buckets = self.find_buckets(key)
			target_buckets = target.find_buckets(key)
			removed_buckets = source_buckets.difference(target_buckets)
			added_buckets = target_buckets.difference(source_buckets)
			if removed_buckets or added_buckets:
				migration_mapping[key] = (source_buckets, removed_buckets, added_buckets)
		return migration_mapping

	def bucket_migration_mapping(self, keys, target):
		"""docstring for bucket_migration_mapping"""
		key_migration_mapping = self.key_migration_mapping(keys, target)
		bucket_migration_mapping = dict((b, []) for b in target.buckets)
		for key, (source_buckets, removed_buckets, target_buckets) in key_migration_mapping.iteritems():
			for target_bucket in target_buckets:
				bucket_migration_mapping[target_bucket].append((key, source_buckets))
		return bucket_migration_mapping

	def range_for_point(self, index):
		"""docstring for range_for_point"""
		buckets = set()
		index_point, index_bucket = self.points[index]
		buckets.add(index_bucket)
		for i, point, bucket in self.points_from(index, reverse=True):
			if len(buckets) >= self.buckets_per_key or len(buckets) >= len(self.buckets):
				return (point, index_point)
			buckets.add(bucket)

	def ranges_for_bucket(self, bucket):
		"""docstring for ranges_for_bucket"""
		bucket_ranges = [self.range_for_point(i) for i, (p, b) in enumerate(self.points) if b == bucket]
		return bucket_ranges

	def _keys_in_bucket(self, sorted_keys, sorted_key_points, bucket):
		"""docstring for _keys_in_bucket"""
		bucket_ranges = self.ranges_for_bucket(bucket)
		bucket_keys = []
		for start, end in bucket_ranges:
			if start == end:
				bucket_keys = keys
				break
			elif start < end:
				start_key_index = bisect.bisect(sorted_key_points, start)
				end_key_index = bisect.bisect(sorted_key_points, end)
				range_keys = sorted_keys[start_key_index:end_key_index]
				bucket_keys.extend(range_keys)
			else: # start > end
				start_key_index = bisect.bisect(sorted_key_points, start)
				end_key_index = bisect.bisect(sorted_key_points, end)
				range_keys = sorted_keys[start_key_index:] + sorted_keys[:end_key_index]
				bucket_keys.extend(range_keys)
		return bucket_keys

	def keys_in_bucket(self, keys, bucket):
		"""docstring for keys_in_bucket"""
		batch_size = 5000
		bucket_keys = []
		for i in range(0, len(keys), batch_size):
			sorted_key_points, sorted_keys = zip(*sorted((self.generate_point(key), key) for key in keys[i:i+batch_size]))
			bucket_keys.extend(self._keys_in_bucket(sorted_keys, sorted_key_points, bucket))
		return bucket_keys

def randomSha(seed):
	sha = hashlib.sha256()
	sha.update(str(seed))
	return sha.hexdigest()

def testRanging():
	ch = ConsistentHash(buckets_per_key=2)
	ch.buckets.append(1)
	ch.buckets.append(2)
	ch.buckets.append(3)
	ch.points.append((10, 1))
	ch.points.append((20, 2))
	ch.points.append((30, 3))
	ch.points.append((40, 1))
	ch.points.append((50, 2))
	ch.points.append((60, 3))
	ch._update_point_index()
	x = 15
	y = 65
	assert(ch.ranges_for_bucket(1) == [(50, 10), (20, 40)])
	assert(ch.ranges_for_bucket(2) == [(60, 20), (30, 50)])
	assert(set(ch._keys_in_bucket(sorted_keys=['x', 'y'], sorted_key_points=[x, y], bucket=1)) == set(['y']))
	assert(set(ch._keys_in_bucket(sorted_keys=['x', 'y'], sorted_key_points=[x, y], bucket=2)) == set(['x', 'y']))
	assert(set(ch._keys_in_bucket(sorted_keys=['x', 'y'], sorted_key_points=[x, y], bucket=3)) == set(['x']))


def testBucketing():
	ch = ConsistentHash(buckets_per_key = 5)
	buckets = [str(i) for i in xrange(0, 17)]
	ch.add_buckets(buckets)
	for	key in xrange(0, 4711):
		buckets_for_key = ch.find_buckets(str(key))
		assert(len(buckets_for_key) == 5)

def testPerf():
	bs = ['b%02d' % i for i in xrange(0, 9)]
	keys = [str(i) for i in range(0, 10000000)]
	ch = ConsistentHash()
	ch.add_buckets(bs)
	ch.keys_in_bucket(keys=keys, bucket=bs[0])

def testMigration():
	"""docstring for testMigration"""
	bs1 = ['b%02d' % i for i in xrange(0, 9)]
	bs2 = ['b%02d' % i for i in xrange(0, 16)]
	keys = ['/path/to/key/%s' % randomSha(i) for i in range(0, 10000)]
	bpk1 = 3
	bpk2 = 4
	buckets_per_key_delta = bpk2 - bpk1
	ch1 = ConsistentHash(buckets_per_key=bpk1)
	ch1.add_buckets(bs1)
	ch2 = ConsistentHash(buckets_per_key=bpk2)
	ch2.add_buckets(bs2)

	print 'Before Migration'
	for s in ch1.buckets:
		print 'Bucket %s: %s keys' % (s, len(list(ch1.keys_in_bucket(keys=keys, bucket=s))))
	print

	print 'After Migration'
	for s in ch2.buckets:
		print 'Bucket %s: %s keys' % (s, len(list(ch2.keys_in_bucket(keys=keys, bucket=s))))
	print

	key_migration_mapping = ch1.key_migration_mapping(keys=keys, target=ch2)
	for key, (source_buckets, deallocated_buckets, target_buckets) in sorted(key_migration_mapping.iteritems()):
		# print 'Key %s: %s => %s. (%s deallocated)' % (key, sorted(source_buckets), sorted(target_buckets), sorted(deallocated_buckets))
		assert(len(target_buckets) - len(deallocated_buckets) >= buckets_per_key_delta)
		assert(len(source_buckets) >= ch1.buckets_per_key)

	bucket_migration_mapping = ch1.bucket_migration_mapping(keys=keys, target=ch2)
	for target_bucket, transfer_list in sorted(bucket_migration_mapping.iteritems()):
		print 'Bucket %s is assigned %d new keys for a total of %d keys' % (target_bucket, len(transfer_list), len(list(ch2.keys_in_bucket(keys=keys, bucket=target_bucket))))
		for key, source_buckets in sorted(transfer_list):
			assert(len(source_buckets) >= ch1.buckets_per_key)
			# print '\t%s <= %s' % (key, sorted(source_buckets))

def main():
	testRanging()
	testBucketing()
	testMigration()
	# testPerf()


if __name__ == "__main__":
	import hotshot
	prof = hotshot.Profile("hotshot.prof")
	prof.runcall(main)
	prof.close()
	# main()
