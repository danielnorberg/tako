# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from configuration cimport Configuration

cdef class Client:
    cdef object coordinator_client
    cdef object configuration_cache
    cdef dict node_clients
    cdef Configuration configuration

    cdef client_for_key(self, str key)
