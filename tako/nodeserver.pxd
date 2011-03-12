# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from models cimport Deployment, Node
from store cimport Store
from configuration cimport Configuration
from httpserver cimport HttpServer

cdef class NodeServer(object):
    cdef str SET_VALUE
    cdef str GET_VALUE
    cdef str GET_TIMESTAMP

    cdef str id
    cdef str var_directory
    cdef object store_file
    cdef str configuration_directory
    cdef object configuration_cache
    cdef public Store store
    cdef dict node_clients
    cdef tuple http_handlers
    cdef object coordinator_client
    cdef Configuration configuration
    cdef Deployment deployment
    cdef bint read_repair_enabled
    cdef Node node
    cdef unsigned int http_port
    cdef object internal_server
    cdef HttpServer http_server
    cdef list coordinators
    cdef object internal_multi_client

    # cdef evaluate_new_configuration(self, new_configuration)
    # cdef set_configuration(self, new_configuration)
    # cdef initialize_node_client_pool(self)
    cdef clients_for_nodes(self, nodes)

    cpdef serve(self)

    cdef fetch_value(self, key, node)
    cdef fetch_timestamps(self, key)
    cdef get_timestamp(self, env)
    cdef propagate(self, object key, object timestamped_value, object target_nodes)
    cdef read_repair(self, object key, object timestamped_value)

    cdef quote(self, key)
    cdef unquote(self, path)
    cpdef store_GET(self, start_response, path, body, env)
    cpdef store_POST(self, start_response, path, body, env)

    cpdef public_get(self, callback, key)
    cpdef public_set(self, callback, key, timestamped_value)
    cpdef public_stat(self, callback, key)
    cpdef internal_get(self, callback, key)
    cpdef internal_set(self, callback, key, timestamped_value)
    cpdef internal_stat(self, callback, key)
