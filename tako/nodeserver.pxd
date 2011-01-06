# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from models cimport Deployment, Node
from store cimport Store
# from socketless.channelserver cimport ChannelServer

cdef class MessageReader(object):
	cdef str message
	cdef unsigned long i
	cpdef str read(self, unsigned long length=*)
	cpdef unsigned long read_int(self)

# cdef class Requests:
# 	cdef GET_VALUE = 'G'
# 	cdef SET_VALUE = 'S'
# 	cdef GET_TIMESTAMP = 'T'
#
# cdef class Responses:
# 	cdef OK = 'K'
# 	cdef NOT_FOUND = 'N'
# 	cdef ERROR = 'E'

# cdef INTERNAL_HANDSHAKE = ('Tako Internal API', 'K')
# cdef PUBLIC_HANDSHAKE = ('Tako Public API', 'K')

cdef class InternalServer

cdef class NodeServer(object):
	cdef str id
	cdef str var_directory
	cdef object store_file
	cdef str configuration_directory
	cdef object configuration_cache
	cdef Store store
	cdef dict node_messengers
	cdef tuple http_handlers
	cdef object configuration
	cdef Deployment deployment
	cdef bint read_repair_enabled
	cdef Node node
	cdef unsigned int http_port
	cdef InternalServer internal_server
	cdef object http_server
	cdef list coordinators

	cdef evaluate_new_configuration(self, new_configuration)
	cdef initialize_messenger_pool(self)
	cdef set_configuration(self, new_configuration)
	cpdef serve(self)
	cdef request_message(self, request, key=*, value=*)
	cdef quote(self, key)
	cdef unquote(self, path)
	cdef get_value(self, key)
	cdef set_value(self, key, timestamped_value)
	cpdef store_GET(self, start_response, path, body, env)
	cpdef store_POST(self, start_response, path, body, env)
	cdef fetch_value(self, key, node)
	cdef fetch_timestamps(self, key)
	cdef get_timestamp(self, env)
	cdef messengers_for_nodes(self, nodes)
	cdef propagate(self, key, timestamped_value, target_nodes=*)
	cdef read_repair(self, key, timestamped_value)


cdef class InternalServer(object):
	cdef tuple listener
	# cdef ChannelServer channel_server
	cdef object channel_server
	cdef NodeServer node_server
	cdef dict internal_handlers
	cdef dict public_handlers

	cdef handshake(self, channel)
	cpdef _flush_loop(channel, flush_queue)
	cpdef handle_connection(self, channel, addr)
	cpdef internal_get_value(self, message, channel)
	cpdef internal_set_value(self, message, channel)
	cpdef internal_get_timestamp(self, message, channel)
	cpdef public_get_value(self, message, channel)
	cpdef public_set_value(self, message, channel)
	cpdef handle_public_connection(self, message, channel)
	cpdef serve(self)
