# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from models cimport Node, Deployment
from consistenthash cimport ConsistentHash

cdef class Configuration(object):
    cdef public object timestamp
    cdef public original_specification
    cdef public deployments
    cdef public str active_deployment_name
    cdef public Deployment active_deployment
    cdef public str target_deployment_name
    cdef public Deployment target_deployment
    cdef public coordinators
    cdef public master_coordinator_id
    cdef public master_coordinator
    # def load(self, specification):
    # def specification(self):
    cpdef find_neighbour_nodes_for_key(self, str key, Node local_node)
    cpdef find_neighbour_nodes_for_node(self, Node local_node)
