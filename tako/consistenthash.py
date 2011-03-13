# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import hashlib
import bisect

class ConsistentHash(object):
    def __init__(self, buckets=[], points_per_bucket=120, buckets_per_key=3, points=None):
        super(ConsistentHash, self).__init__()
        self.buckets = list(buckets)
        self.points_per_bucket = points_per_bucket
        self.buckets_per_key = buckets_per_key
        self.points = []
        self.__neighbour_buckets_by_bucket_hash = {}
        self.__neighbour_buckets_by_point_index = []
        self.__bisect = bisect.bisect
        if points:
            self.points = points
        else:
            self.points = sorted([(self.__generate_point("%s-%s" % (hash(bucket), i)), bucket) for bucket in self.buckets for i in xrange(self.points_per_bucket)])
        self.__point_index = [point[0] for point in self.points]
        self.__point_count = len(self.points)
        for bucket in self.buckets:
            self.__neighbour_buckets_by_bucket_hash[hash(bucket)] = frozenset(self.__neighbour_buckets_for_bucket(bucket))
        for index, (coordinate, bucket) in enumerate(self.points):
            self.__neighbour_buckets_by_point_index.append(frozenset(self.__buckets_for_point_index(index)))

    def __generate_point(self, key):
        key_hash = hashlib.md5()
        key_hash.update(key)
        return int(key_hash.hexdigest()[:8], 16)

    def __buckets_for_point_index(self, index):
        buckets = set()
        buckets.add(self.points[index][1])
        n = 1
        while n < self.buckets_per_key and n < len(self.buckets):
            index += 1
            point, bucket = self.points[index % self.__point_count]
            buckets.add(bucket)
            n = len(buckets)
        return buckets

    def __neighbour_buckets_for_bucket(self, bucket):
        neighbour_buckets = set()
        bucket_points = [(i, p, b) for i, (p, b) in enumerate(self.points) if b == bucket]
        for i, p, b in bucket_points:
            point_neighbour_buckets = set()
            n = 0
            while n < self.buckets_per_key and n < len(self.buckets):
                i -= 1
                neighbour_point, neighbour_bucket = self.points[i % self.__point_count]
                point_neighbour_buckets.add(neighbour_bucket)
                n = len(point_neighbour_buckets)
            neighbour_buckets.update(point_neighbour_buckets)
        return neighbour_buckets

    def find_buckets(self, key):
        index = self.__bisect(self.__point_index, self.__generate_point(key))
        return self.__neighbour_buckets_by_point_index[index % self.__point_count]

    def find_neighbour_buckets(self, bucket):
        return self.__neighbour_buckets_by_bucket_hash[hash(bucket)]

    def __repr__(self):
        return 'ConsistentHash(buckets_per_key=%d, points_per_pucket=%d)' % (self.buckets_per_key, self.points_per_bucket)

    def __str__(self):
        return repr(self) + ''.join(['\n\t%08x: %s' % (point, str(bucket)) for point, bucket in self.points])
