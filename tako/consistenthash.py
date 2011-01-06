import hashlib
import bisect
import unittest
from utils import testcase

class ConsistentHash(object):
    """Implements a consistent hash"""
    def __init__(self, buckets=[], points_per_bucket=120, buckets_per_key=3, points=None):
        super(ConsistentHash, self).__init__()
        self.points_per_bucket = points_per_bucket
        self.buckets = []
        self.points = []
        self.buckets_per_key = buckets_per_key
        self.buckets = list(buckets)
        if points:
            self.points = points
        else:
            self.points = sorted((self.generate_point("%s-%s" % (id(bucket), i)), bucket) for bucket in self.buckets for i in xrange(self.points_per_bucket))
            self.point_index = [point[0] for point in self.points]

    def find_bucket_point(self, key):
        """docstring for find_bucket_point"""
        key_point = self.generate_point(key)
        i = bisect.bisect(self.point_index, key_point) % len(self.points)
        return i, self.points[i]

    def bucket_points_from_index(self, index, reverse=False):
        """docstring for bucket_points_from_index"""
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
        len_points = len(self.points)
        index = bisect.bisect(self.point_index, self.generate_point(key)) % len_points
        buckets.add(self.points[index][1])
        n = 1
        while n < self.buckets_per_key and n < len(self.buckets):
            index += 1
            point, bucket = self.points[index % len_points]
            buckets.add(bucket)
            n = len(buckets)
        return buckets

    def find_neighbour_buckets(self, bucket):
        """docstring for find_neighbour_buckets"""
        neighbour_buckets = set()
        bucket_points = [(i, p, b) for i, (p, b) in enumerate(self.points) if b == bucket]
        for i, p, b in bucket_points:
            point_neighbour_buckets = set()
            for j, neighbout_bucket_point, neighbour_bucket in self.bucket_points_from_index(i, reverse=True):
                point_neighbour_buckets.add(neighbour_bucket)
                if len(point_neighbour_buckets) >= self.buckets_per_key or len(point_neighbour_buckets) >= len(self.buckets):
                    break
            neighbour_buckets.update(point_neighbour_buckets)
        return neighbour_buckets

    def generate_point(self, key):
        """docstring for generate_point"""
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
        for     key in keys:
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
        for i, point, bucket in self.bucket_points_from_index(index, reverse=True):
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

    def keys_in_bucket(self, keys, bucket, points=None):
        """docstring for keys_in_bucket"""
        batch_size = 5000
        bucket_keys = []
        for i in range(0, len(keys), batch_size):
            if points:
                sorted_key_points, sorted_keys = zip(*sorted(zip(points[i:i+batch_size],keys[i:i+batch_size])))
            else:
                sorted_key_points, sorted_keys = zip(*sorted((self.generate_point(key), key) for key in keys[i:i+batch_size]))
            bucket_keys.extend(self._keys_in_bucket(sorted_keys, sorted_key_points, bucket))
        return bucket_keys

    def __repr__(self):
        """docstring for __repr__"""
        return 'ConsistentHash(buckets_per_key=%d, points_per_pucket=%d)' % (self.buckets_per_key, self.points_per_bucket)

    def __str__(self):
        """docstring for __repr__"""
        return repr(self) + ''.join('\n\t%08x: %s' % (point, str(bucket)) for point, bucket in self.points)


class TestConsistentHash(testcase.TestCase):
    """docstring for TestConsistentHash"""

    def randomSha(self, seed):
        sha = hashlib.sha256()
        sha.update(str(seed))
        return sha.hexdigest()

    def testRanging(self):
        ch = ConsistentHash(buckets=[1, 2, 4], points=[
                (10, 1),
                (20, 2),
                (30, 3),
                (40, 1),
                (50, 2),
                (60, 3),
        ], buckets_per_key=2)
        # ch.buckets.append(1)
        # ch.buckets.append(2)
        # ch.buckets.append(3)
        # ch._update_point_index()
        x = 15
        y = 65
        self.assertEqual(ch.ranges_for_bucket(1), [(50, 10), (20, 40)])
        self.assertEqual(ch.ranges_for_bucket(2), [(60, 20), (30, 50)])
        self.assertEqual(set(ch._keys_in_bucket(sorted_keys=['x', 'y'], sorted_key_points=[x, y], bucket=1)), set(['y']))
        self.assertEqual(set(ch._keys_in_bucket(sorted_keys=['x', 'y'], sorted_key_points=[x, y], bucket=2)), set(['x', 'y']))
        self.assertEqual(set(ch._keys_in_bucket(sorted_keys=['x', 'y'], sorted_key_points=[x, y], bucket=3)), set(['x']))
        self.assertEqual(ch.find_neighbour_buckets(1), set([2, 3]))
        self.assertEqual(ch.find_neighbour_buckets(2), set([1, 3]))
        self.assertEqual(ch.find_neighbour_buckets(3), set([1, 2]))

    def testBucketing(self):
        buckets = [str(i) for i in xrange(0, 17)]
        ch = ConsistentHash(buckets, buckets_per_key=5)
        for     key in xrange(0, 4711):
            buckets_for_key = ch.find_buckets(str(key))
            self.assertEqual(len(buckets_for_key), 5)

    # def testPerf(self):
    #       bs = ['b%02d' % i for i in xrange(0, 9)]
    #       keys = [str(i) for i in range(0, 10000000)]
    #       ch = ConsistentHash()
    #       points = list(ch.generate_points(keys=keys))
    #       ch.add_buckets(bs)
    #       ch.keys_in_bucket(keys=keys, bucket=bs[0], points=points)
    #
    # def testPerf(self):
    #       bs = ['b%02d' % i for i in xrange(100)]
    #       keys = [str(i) for i in xrange(1000)]
    #       ch = ConsistentHash()
    #       # points = list(ch.generate_points(keys=keys))
    #       ch.add_buckets(bs)
    #       for i in xrange(1000000):
    #               ch.find_buckets(keys[i%1000])

    def testMigration(self):
        """docstring for testMigration"""
        bs1 = ['b%02d' % i for i in xrange(0, 9)]
        bs2 = ['b%02d' % i for i in xrange(0, 16)]
        keys = ['/path/to/key/%s' % self.randomSha(i) for i in range(0, 10000)]
        bpk1 = 3
        bpk2 = 4
        buckets_per_key_delta = bpk2 - bpk1
        ch1 = ConsistentHash(bs1, buckets_per_key=bpk1)
        ch2 = ConsistentHash(bs2, buckets_per_key=bpk2)

        points = list(ch1.generate_points(keys))

        print 'Before Migration'
        for s in ch1.buckets:
            print 'Bucket %s: %s keys, %s neighbours' % (s, len(list(ch1.keys_in_bucket(keys=keys, bucket=s, points=points))), len(ch1.find_neighbour_buckets(bucket=s)))
        print

        print 'After Migration'
        for s in ch2.buckets:
            print 'Bucket %s: %s keys, %s neighbours' % (s, len(list(ch2.keys_in_bucket(keys=keys, bucket=s, points=points))), len(ch2.find_neighbour_buckets(bucket=s)))
        print

        key_migration_mapping = ch1.key_migration_mapping(keys=keys, target=ch2)
        for key, (source_buckets, deallocated_buckets, target_buckets) in sorted(key_migration_mapping.iteritems()):
            self.assertTrue(len(target_buckets) - len(deallocated_buckets) >= buckets_per_key_delta)
            self.assertTrue(len(source_buckets) >= ch1.buckets_per_key)

        bucket_migration_mapping = ch1.bucket_migration_mapping(keys=keys, target=ch2)
        for target_bucket, transfer_list in sorted(bucket_migration_mapping.iteritems()):
            print 'Bucket %s is assigned %d new keys for a total of %d keys' % (target_bucket, len(transfer_list), len(list(ch2.keys_in_bucket(keys=keys, bucket=target_bucket, points=points))))
            for key, source_buckets in sorted(transfer_list):
                self.assertTrue(len(source_buckets) >= ch1.buckets_per_key)

if __name__ == "__main__":
    unittest.main()
