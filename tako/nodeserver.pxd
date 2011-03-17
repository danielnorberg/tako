# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from tako.models cimport Deployment, Node
from tako.configuration cimport Configuration
from tako.store cimport Store
from tako.utils.httpserver cimport HttpServer

cdef class NodeServer(object):
    cdef str id
    cdef Node node

    cdef Store __store
    cdef dict __node_clients
    cdef object __internal_cluster_client
    cdef Configuration __configuration
    cdef object __configuration_controller

    cdef bint __read_repair_enabled
    cdef bint __background_repair_enabled
    cdef float __background_repair_interval_seconds

    cdef object __server

    cdef __read_repair(self, object key, long timestamp, object value)
    cdef __fetch_timestamps(self, object key)
    cdef __clients_for_nodes(self, object node_ids)

    cdef __fetch_value(self, object key, object node_id)
    cdef __get_timestamp(self, env)
    cdef __propagate(self, object key, long timestamp, object value, object target_nodes)

    cdef __quote(self, key)
    cdef __unquote(self, path)

