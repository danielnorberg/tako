import logging
import gevent
from gevent.wsgi import WSGIServer

class BadRequest(object):
	"""docstring for BadRequest"""
	def __init__(self, description=''):
		super(BadRequest, self).__init__()
		self.description = description

	def __str__(self):
		"""docstring for __str__"""
		return repr(self)

	def __repr__(self):
		"""docstring for __repr__"""
		return "BadRequest('%s')" % self.description

class HttpServer(object):
	"""docstring for HttpServer"""
	def __init__(self):
		super(HttpServer, self).__init__()

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
		"""docstring for serve"""
		logging.info('Listening on port %d', self.port)
		self.wsgi_server = WSGIServer(('', self.port), self.handle_request)
		self.wsgi_server.serve_forever()
