import logging
from syncless import coio
from syncless import wsgi

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
		def _handle(env, start_response):
			return self.handle_request(env, start_response)
		logging.info('Listening on port %d', self.port)
		wsgi.RunHttpServer(_handle, ('', self.port))

from utils.testcase import TestCase
class TestHttpServer(TestCase):

	class DummyHttpServer(HttpServer):
		"""docstring for DummyHttpServer"""
		def __init__(self, port):
			super(TestHttpServer.DummyHttpServer, self).__init__()
			self.handlers = (
				('/', {'GET':self.GET}),
			)
			self.port = port

		def GET(self, start_response, path, body, env):
			"""docstring for GET"""
			start_response("200 OK", [('Content-Type', 'text/html')])
			return ["TestHttpServer"]

	def testServer(self):
		import urllib
		from syncless import patch
		patch.patch_socket()
		"""docstring for testServer"""
		s = TestHttpServer.DummyHttpServer(4711)
		t = coio.stackless.tasklet(s.serve)()
		coio.stackless.schedule()
		stream = urllib.urlopen('http://127.0.0.1:4711/')
		body = stream.read()
		stream.close()
		assert body == "TestHttpServer"
		t.kill()

if __name__ == '__main__':
	import unittest
	unittest.main()