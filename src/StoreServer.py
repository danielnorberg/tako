import platform
from twisted.web import server, resource
import KeyValueStoreResource

if platform.system() == 'Linux':
	from twisted.internet import epollreactor
	epollreactor.install()

from twisted.internet import reactor

site = server.Site(KeyValueStoreResource.KeyValueStoreResource())
reactor.listenTCP(8080, site)
reactor.run()