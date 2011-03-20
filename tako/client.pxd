# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from configuration cimport Configuration

cdef class Client:
    cdef int max_retries
    cdef float retry_interval
    cdef dict __node_clients
    cdef object __configuration_controller
    cdef Configuration __configuration

    cdef __connected_clients_for_key(self, str key)

    cpdef set(self, object key, long timestamp, object value)
    cpdef get(self, object key)
    cpdef stat(self, object key)
