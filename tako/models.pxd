# from consistenthash import ConsistentHash

cdef class Node(object):
	cdef public str id
	cdef public str bucket_id
	cdef public str address
	cdef public unsigned int http_port
	cdef public unsigned int raw_port
	cpdef raw_address(self)
	cpdef store_url(self)
	cpdef stat_url(self)

cdef class Bucket(object):
	cdef public str id
	cdef public dict nodes

cdef class Deployment(object):
	cdef public dict original_specification
	cdef public str name
	cdef public dict buckets
	cdef public dict nodes
	cdef public object consistent_hash
	cdef public bint read_repair_enabled
	cpdef siblings(self, node_id)
	cpdef specification(self)

cdef class Coordinator(object):
	cdef public str id
	cdef public str address
	cdef public unsigned int port
	cpdef configuration_url(self)
