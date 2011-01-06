import hashlib
import unittest
from utils import testcase


from consistenthash import ConsistentHash

class TestConsistentHash(testcase.TestCase):
    """docstring for TestConsistentHash"""

    def randomSha(self, seed):
        sha = hashlib.sha256()
        sha.update(str(seed))
        return sha.hexdigest()

    def testRanging(self):
        ch = ConsistentHash(buckets=[1, 2, 3], points=[
                (10, 1),
                (20, 2),
                (30, 3),
                (40, 1),
                (50, 2),
                (60, 3),
        ], buckets_per_key=2)
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
    def testPerf(self):
        bs = ['b%02d' % i for i in xrange(100)]
        keys = [str(i) for i in xrange(10000)]
        ch = ConsistentHash(bs)
        for i in xrange(10000):
            ch.find_buckets(keys[i%1000])

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
            print 'Bucket %s: %s keys, %s neighbours' % (s, len(list(ch1.keys_in_bucket(keys=keys, bucket=s, points=points))), len(ch1.find_neighbour_buckets(s)))
        print

        print 'After Migration'
        for s in ch2.buckets:
            print 'Bucket %s: %s keys, %s neighbours' % (s, len(list(ch2.keys_in_bucket(keys=keys, bucket=s, points=points))), len(ch2.find_neighbour_buckets(s)))
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
