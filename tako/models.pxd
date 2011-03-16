# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from consistenthash cimport ConsistentHash

cdef class Node(object):
    cdef public str id
    cdef public str bucket_id
    cdef public str address
    cdef public int http_port
    cdef public int raw_port
    cpdef raw_address(self)
    cpdef store_url(self)
    cpdef stat_url(self)

cdef class Bucket(object):
    cdef public str id
    cdef public dict nodes

cdef class Deployment(object):
    cdef public dict original_representation
    cdef public str name
    cdef public dict buckets
    cdef public dict nodes
    cdef public ConsistentHash consistent_hash
    cdef public bint read_repair_enabled
    cdef public bint background_healing_enabled
    cdef public object background_healing_interval
    cdef public float background_healing_interval_seconds
    cpdef siblings(self, str node_id)
    cpdef buckets_for_key(self, str key)
    cpdef representation(self)

cdef class Coordinator(object):
    cdef public str id
    cdef public str address
    cdef public int port
    cpdef configuration_url(self)
