cdef class HttpServer(object):
	cdef str address
	cdef unsigned int port
	cdef object handlers
	cdef unsigned int listen_queue_size
	cdef object server_socket

	cpdef handle_request(self, env, start_response)
	cpdef serve(self)
