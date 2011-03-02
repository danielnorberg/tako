# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

cdef class ConsistentHash(object):
    cdef public points_per_bucket
    cdef public buckets
    cdef points
    cdef unsigned long point_count
    cdef public buckets_per_key
    cdef point_index
    cdef list neighbour_buckets_by_point_index
    cdef dict neighbour_buckets_by_bucket_hash
    cdef object bisect

    cpdef find_bucket_point(self, key)
    cdef _find_buckets_for_index(self, index)
    cpdef find_buckets(self, key)
    cdef inline _find_neighbour_buckets_by_point_index(self, unsigned long long index)
    cpdef find_neighbour_buckets(self, bucket)
    cpdef generate_point(self, key)
    cpdef generate_points(self, keys)
    cpdef key_migration_mapping(self, keys, target)
    cpdef bucket_migration_mapping(self, keys, target)
    cpdef range_for_point(self, index)
    cpdef ranges_for_bucket(self, bucket)
    cpdef _keys_in_bucket(self, sorted_keys, sorted_key_points, bucket)
    cpdef keys_in_bucket(self, keys, bucket, points=*)
