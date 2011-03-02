# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import hashlib
import bisect

class ConsistentHash(object):
    """Implements a consistent hash"""
    def __init__(self, buckets=[], points_per_bucket=120, buckets_per_key=3, points=None):
        super(ConsistentHash, self).__init__()
        self.points_per_bucket = points_per_bucket
        self.buckets = []
        self.neighbour_buckets_by_bucket_hash = {}
        self.neighbour_buckets_by_point_index = []
        self.points = []
        self.buckets_per_key = buckets_per_key
        self.buckets = list(buckets)
        self.bisect = bisect.bisect
        if points:
            self.points = points
        else:
            self.points = sorted([(self.generate_point("%s-%s" % (hash(bucket), i)), bucket) for bucket in self.buckets for i in xrange(self.points_per_bucket)])
            self.point_index = [point[0] for point in self.points]
        self.point_count = len(self.points)
        for bucket in self.buckets:
            self.neighbour_buckets_by_bucket_hash[hash(bucket)] = frozenset(self._find_neighbour_buckets(bucket))
        for index, (coordinate, bucket) in enumerate(self.points):
            self.neighbour_buckets_by_point_index.append(frozenset(self._find_buckets_for_index(index)))

    def find_bucket_point(self, key):
        """docstring for find_bucket_point"""
        key_point = self.generate_point(key)
        i = self.bisect(self.point_index, key_point) % self.point_count
        return i, self.points[i]

    def _find_buckets_for_index(self, index):
        buckets = set()
        buckets.add(self.points[index][1])
        n = 1
        while n < self.buckets_per_key and n < len(self.buckets):
            index += 1
            point, bucket = self.points[index % self.point_count]
            buckets.add(bucket)
            n = len(buckets)
        return buckets

    def _find_neighbour_buckets_by_point_index(self, index):
        return self.neighbour_buckets_by_point_index[index % self.point_count]

    def find_buckets(self, key):
        """docstring for find_buckets"""
        index = self.bisect(self.point_index, self.generate_point(key))
        return self._find_neighbour_buckets_by_point_index(index)

    def find_neighbour_buckets(self, bucket):
        return self.neighbour_buckets_by_bucket_hash[hash(bucket)]

    def _find_neighbour_buckets(self, bucket):
        """docstring for find_neighbour_buckets"""
        neighbour_buckets = set()
        bucket_points = [(i, p, b) for i, (p, b) in enumerate(self.points) if b == bucket]
        for i, p, b in bucket_points:
            point_neighbour_buckets = set()
            n = 0
            while n < self.buckets_per_key and n < len(self.buckets):
                i -= 1
                neighbour_point, neighbour_bucket = self.points[i % self.point_count]
                point_neighbour_buckets.add(neighbour_bucket)
                n = len(point_neighbour_buckets)
            neighbour_buckets.update(point_neighbour_buckets)
        return neighbour_buckets

    def generate_point(self, key):
        """docstring for generate_point"""
        key_hash = hashlib.md5()
        key_hash.update(key)
        return int(key_hash.hexdigest()[:8], 16)

    def generate_points(self, keys):
        """docstring for generate_points"""
        return [self.generate_point(key) for key in keys]

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
        while len(buckets) < self.buckets_per_key and len(buckets) < len(self.buckets):
            index -= 1
            point, bucket = self.points[index % self.point_count]
            buckets.add(bucket)
        return (point, index_point)

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
                bucket_keys = sorted_keys
                break
            elif start < end:
                start_key_index = self.bisect(sorted_key_points, start)
                end_key_index = self.bisect(sorted_key_points, end)
                range_keys = sorted_keys[start_key_index:end_key_index]
                bucket_keys.extend(range_keys)
            else: # start > end
                start_key_index = self.bisect(sorted_key_points, start)
                end_key_index = self.bisect(sorted_key_points, end)
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
                sorted_key_points, sorted_keys = zip(*sorted([(self.generate_point(key), key) for key in keys[i:i+batch_size]]))
            bucket_keys.extend(self._keys_in_bucket(sorted_keys, sorted_key_points, bucket))
        return bucket_keys

    def __repr__(self):
        """docstring for __repr__"""
        return 'ConsistentHash(buckets_per_key=%d, points_per_pucket=%d)' % (self.buckets_per_key, self.points_per_bucket)

    def __str__(self):
        """docstring for __repr__"""
        return repr(self) + ''.join(['\n\t%08x: %s' % (point, str(bucket)) for point, bucket in self.points])
