# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

cdef class ConsistentHash(object):
    cdef public buckets
    cdef public points_per_bucket
    cdef public buckets_per_key
    cdef public points

    cdef unsigned long __point_count
    cdef list __point_index
    cdef list __neighbour_buckets_by_point_index
    cdef dict __neighbour_buckets_by_bucket_hash
    cdef object __bisect

    cdef __generate_point(self, key)
    cdef __buckets_for_point_index(self, index)
    cdef __neighbour_buckets_for_bucket(self, bucket)

    cpdef find_buckets(self, key)
    cpdef find_neighbour_buckets(self, bucket)
