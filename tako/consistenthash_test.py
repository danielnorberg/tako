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
        self.assertEqual(ch.find_neighbour_buckets(1), set([2, 3]))
        self.assertEqual(ch.find_neighbour_buckets(2), set([1, 3]))
        self.assertEqual(ch.find_neighbour_buckets(3), set([1, 2]))

    def testBucketing(self):
        buckets = [str(i) for i in xrange(0, 17)]
        ch = ConsistentHash(buckets, buckets_per_key=5)
        for     key in xrange(0, 4711):
            buckets_for_key = ch.find_buckets(str(key))
            self.assertEqual(len(buckets_for_key), 5)

    def testPerf(self):
        bs = ['b%02d' % i for i in xrange(100)]
        keys = [str(i) for i in xrange(10000)]
        ch = ConsistentHash(bs)
        for i in xrange(10000):
            ch.find_buckets(keys[i%1000])

if __name__ == "__main__":
    unittest.main()
