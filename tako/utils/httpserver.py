# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import logging
import socket

from syncless import coio
from syncless import wsgi

class BadRequest(object):
    def __init__(self, description=''):
        super(BadRequest, self).__init__()
        self.description = description

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "BadRequest('%s')" % self.description

class HttpServer(object):
    def __init__(self, listener, handlers, listen_queue_size=1024):
        super(HttpServer, self).__init__()
        self.address, self.port = listener
        self.handlers = handlers
        self.listen_queue_size = listen_queue_size
        self.server_socket = None

    def handle_request(self, env, start_response):
        method = env['REQUEST_METHOD']
        path = env['PATH_INFO']
        body = env['wsgi.input']
        for prefix, methods in self.handlers:
            if path.startswith(prefix):
                sub_path = path[len(prefix):]
                method_handler = methods.get(method, None)
                if method_handler:
                    try:
                        return method_handler(start_response, sub_path, body, env)
                    except BadRequest, e:
                        start_response('400 Bad Request', [e.description])
                        return ['']
                else:
                    start_response('405 Method Not Allowed', [('Allow', ','.join(methods.keys()))])
        start_response('404 Not Found', [])
        return ['']

    def serve(self):
        self.server_socket = coio.nbsocket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.address, self.port))
        self.server_socket.listen(self.listen_queue_size)
        if __debug__: logging.debug('listening on %r' % (self.server_socket.getsockname(),))
        wsgi.WsgiListener(self.server_socket, self.handle_request)
