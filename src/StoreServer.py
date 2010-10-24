import platform
from twisted.web import server, resource
import StoreResource

if platform.system() == 'Linux':
	from twisted.internet import epollreactor
	epollreactor.install()

from twisted.internet import reactor

class StoreServer(object):
	"""docstring for KeyValueStoreServer"""
	def __init__(self, port=4711):
		super(StoreServer, self).__init__()
		self.port = port

	def start(self):
		"""docstring for start"""
		site = server.Site(StoreResource.StoreResource())
		reactor.listenTCP(self.port, site)
		reactor.run()

if __name__ == '__main__':
	storeServer = StoreServer()
	storeServer.start()