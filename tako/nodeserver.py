import urllib
import argparse
import logging
from utils import debug
import os, sys
import email.utils
import struct

from syncless import coio

from syncless import patch
patch.patch_socket()

from socketless.messenger import Messenger
from socketless.channelserver import ChannelServer
from socketless.channel import DisconnectedException
from socketless.broadcast import Broadcast

from utils.timestamp import Timestamp
# from utils import runner
from utils import convert
# from utils import http

import httpserver
import paths
from store import Store
import configuration
from configurationcache import ConfigurationCache
from configuration import Coordinator
# from coordinatorclient import CoordinatorClient

class MessageReader(object):
	"""docstring for MessageReader"""
	def __init__(self, message):
		super(MessageReader, self).__init__()
		self.message = message
		self.i = 0

	def read(self, length=0):
		"""docstring for read"""
		assert self.message
		assert self.i + length <= len(self.message)
		if not length:
			length = len(self.message) - self.i
		if length > 1024:
			data = buffer(self.message, self.i, length)
		else:
			data = self.message[self.i:self.i+length]
		self.i += length
		return data

	def read_int(self):
		return struct.unpack('!L', self.read(4))[0]

class Requests:
	GET_VALUE = 'G'
 	SET_VALUE = 'S'
	GET_TIMESTAMP = 'T'

class Responses:
	OK = 'K'
	NOT_FOUND = 'N'

class InternalServer(object):
	"""docstring for Server"""
	def __init__(self, listener, node_server):
		super(InternalServer, self).__init__()
		self.listener = listener
		self.channel_server = ChannelServer(self.listener, handle_connection=self.handle_connection)
		self.node_server = node_server

	def handle_connection(self, channel, addr):
		"""docstring for handle_connection"""
		try:
			while True:
				message = channel.recv()
				if not message:
					break
				message = MessageReader(message)
				operation = message.read(1)
				if operation == Requests.GET_VALUE:
					key_length = message.read_int()
					key = message.read()
					timestamped_value = self.node_server.store.get_timestamped(key)
					if timestamped_value:
						channel.send(Responses.OK + timestamped_value)
					else:
						channel.send(Responses.NOT_FOUND)
				if operation == Requests.SET_VALUE:
					key_length = message.read_int()
					value_length = message.read_int()
					key = message.read(key_length)
					value = message.read(value_length)
					self.node_server.store.set_timestamped(key, value)
					channel.send(Responses.OK)
				if operation == Requests.GET_TIMESTAMP:
					key_length = message.read_int()
					key = message.read(key_length)
					value, timestamp = self.node_server.store.get(key)
					if timestamp:
						# TODO, send a header!
						channel.send(Responses.OK + struct.pack('!Q', timestamp.microseconds))
					else:
						channel.send(Responses.NOT_FOUND)
				channel.flush()
		except DisconnectedException:
			logging.info('client %s disconnected', addr)
		except Exception, e:
			logging.exception(e)
		finally:
			try:
				channel.close()
			except Exception, e:
				logging.exception(e)

	def serve(self):
		logging.info("Listening on %s", self.listener)
		self.channel_server.serve()


class NodeServer(object):
	def __init__(self, node_id, store_file=None, explicit_configuration=None, coordinators=[], var_directory='var'):
		super(NodeServer, self).__init__()
		self.id = node_id
		self.var_directory = os.path.join(paths.home, var_directory)
		self.store_file = store_file or os.path.join(self.var_directory, 'data', '%s.tcb' % self.id)
		self.configuration_directory = os.path.join(self.var_directory, 'etc')
		self.configuration_cache = ConfigurationCache(self.configuration_directory, 'nodeserver-%s' % self.id)
		self.store = Store(self.store_file)
		self.store.open()
		self.node_messengers = {}
		self.http_handlers = (
			('/store/', {'GET':self.store_GET, 'POST':self.store_POST}),
			# ('/internal/', {'GET':self.internal_GET, 'POST':self.internal_POST}),
			# ('/stat/', {'GET':self.stat_GET}),
		)
		# self.coordinator_client = CoordinatorClient(coordinators=coordinators, callbacks=[self.evaluate_new_configuration], interval=30)

		self.configuration = None
		if explicit_configuration:
			logging.debug('using explicit configuration')
			self.evaluate_new_configuration(explicit_configuration)
		else:
			cached_configuration = self.configuration_cache.get_configuration()
			if cached_configuration:
				logging.debug('using cached configuration')
				self.evaluate_new_configuration(cached_configuration)

	def evaluate_new_configuration(self, new_configuration):
		"""docstring for evaluate_new_configuration"""
		if not self.configuration or new_configuration.timestamp > self.configuration.timestamp:
			self.set_configuration(new_configuration)

	def set_configuration(self, new_configuration):
		"""docstring for configuration"""
		logging.debug(new_configuration)
		self.configuration = new_configuration
		self.deployment = self.configuration.active_deployment
		self.read_repair_enabled = self.configuration.active_deployment.read_repair_enabled
		self.node = self.deployment.nodes[self.id]
		self.siblings = self.deployment.siblings(self.id)
		self.http_port = self.node.http_port
		self.configuration_cache.cache_configuration(self.configuration)
		for messenger in self.node_messengers.values():
			messenger.close()
		neighbour_nodes = self.configuration.find_neighbour_nodes_for_node(self.node)
		self.node_messengers = dict((node.id, Messenger((node.address, node.raw_port))) for node in neighbour_nodes)

	def serve(self):
		"""docstring for server"""
		# self.coordinator_client.start()
		logging.info('Checking Configuration.')
		while not self.configuration:
			logging.debug('Waiting for configuration.')
			coio.sleep(1)
		self.internal_server = InternalServer(listener=(self.node.address, self.node.raw_port), node_server=self)
		self.internal_server.serve()
		self.http_server = httpserver.HttpServer(listener=(self.node.address, self.node.http_port), handlers=self.http_handlers)
		self.http_server.serve()

	def request_message(self, request, key=None, value=None):
		"""docstring for message"""
		if not key:
			return request
		else:
			if value:
				fragments = (struct.pack('!cLL', request, len(key), len(value)), key, value)
			else:
				fragments = (struct.pack('!cL', request, len(key)), key)
			return ''.join(fragments)

	def quote(self, key):
		"""docstring for quote"""
		return urllib.quote_plus(key, safe='/&')

	def unquote(self, path):
		"""docstring for unquote"""
		return urllib.unquote_plus(path)

	def store_GET(self, start_response, path, body, env):
		key = self.unquote(path)
		timestamped_value = self.store.get_timestamped(key)

		logging.debug('key: %s', key)

		if self.read_repair_enabled:
			timestamped_value = self.read_repair(key, timestamped_value)

		if timestamped_value:
			value, timestamp = self.store.unpack_timestamped_data(timestamped_value)
			start_response('200 OK', [
				('Content-Type', 'application/octet-stream'),
				('Last-Modified', email.utils.formatdate(timestamp.to_seconds())),
				('X-TimeStamp', str(timestamp)),
			])
			return [value]
		else:
			start_response('404 Not Found', [])
			return ['']

	def store_POST(self, start_response, path, body, env):
		key = self.unquote(path)
		value = body.read()
		timestamp = self.get_timestamp(env)
		logging.debug("key: %s, timestamp: %s", key, timestamp)
		timestamped_value = self.store.pack_timestamped_data(value, timestamp)
		self.store.set_timestamped(key, timestamped_value)
		self.propagate(key, timestamped_value)
		start_response('200 OK', [
			('Content-Type', 'application/octet-stream'),
			('X-TimeStamp', str(timestamp)),
		])
		return ['']

	def fetch_value(self, key, node):
		"""docstring for fetch_value"""
		logging.debug('key: %s, node: %s', key, node)
		messengers = self.messengers_for_nodes([node])
		message = self.request_message(Requests.GET_VALUE, key)
		broadcast = Broadcast(messengers)
		[reply] = broadcast.send(message)
		reply = MessageReader(reply)
		if reply.read(1) == Responses.OK:
			return reply.read()
		else:
			return None

	def fetch_timestamps(self, key):
		"""docstring for fetch_timestamps"""
		logging.debug('key: %s', key)
		neighbour_nodes = self.configuration.find_neighbour_nodes_for_key(key, self.node)
		messengers = self.messengers_for_nodes(neighbour_nodes)
		message = self.request_message(Requests.GET_TIMESTAMP, key)
		broadcast = Broadcast(messengers)
		replies = broadcast.send(message)
		timestamps = [(self.store.read_timestamp(timestamp_data[1:]) if timestamp_data and timestamp_data[0] == Responses.OK else None, node) for timestamp_data, node in replies]
		return timestamps

	def get_timestamp(self, env):
		"""docstring for get_timestamp"""
		try:
			return Timestamp.try_loads(env.get('HTTP_X_TIMESTAMP', None)) or Timestamp.now()
		except ValueError:
			raise httpserver.BadRequest()

	def messengers_for_nodes(self, nodes):
		"""docstring for messengers_for_nodes"""
		messengers = [(node, self.node_messengers[node.id]) for node in nodes]
		return messengers

	def propagate(self, key, timestamped_value, target_nodes = []):
		"""docstring for propagate"""
		logging.debug('key: %s', key)
		if not target_nodes:
			target_nodes = self.configuration.find_neighbour_nodes_for_key(key, self.node)
		if not target_nodes:
			return
		message = self.request_message(Requests.SET_VALUE, key, timestamped_value)
		messengers = self.messengers_for_nodes(target_nodes)
		broadcast = Broadcast(messengers)
		results = broadcast.send(message)

	def read_repair(self, key, timestamped_value):
		"""docstring for read_repair"""
		timestamp = self.store.read_timestamp(timestamped_value) if timestamped_value else None
		logging.debug('key: %s, timestamp: %s', key, timestamp)
		remote_timestamps = self.fetch_timestamps(key)
		logging.debug('remote: %s', remote_timestamps)
		newer = [(remote_timestamp, node) for remote_timestamp, node in remote_timestamps if remote_timestamp > timestamp]
		logging.debug('newer: %s', newer)
		if newer:
			latest_timestamp, latest_node = newer[-1]
			latest_timestamped_value = self.fetch_value(key, latest_node)
			if latest_timestamped_value:
				timestamped_value = latest_timestamped_value
				timestamp = self.store.read_timestamp(latest_timestamped_value)
				self.store.set_timestamped(key, latest_timestamped_value)

		older = [(remote_timestamp, node) for remote_timestamp, node in remote_timestamps if remote_timestamp <	timestamp]
		logging.debug('older: %s', older)
		if older:
			older_nodes = [node for (remote_timestamp, node) in older]
			self.propagate(key, timestamped_value, older_nodes)

		return timestamped_value

def _main(args):
	level = logging.DEBUG if args.debug else logging.INFO
	debug.configure_logging('nodeserver', level)

	if args.debug:
		logging.debug('debugging enabled')

	config = None
	if args.config:
		config = configuration.try_load_file(args.config)
		if not config:
			print >> sys.stderr, 'Failed to load configuration file.'
			exit(-1)
		if args.id not in config.active_deployment.nodes:
			print >> sys.stderr, 'Configuration for Node (id = %s) not found in configuration' % args.id
			exit(-1)

	print 'Tako Node'
	print '-' * 80
	print 'Node id: %s' % args.id

	coordinators = []
	if args.coordinator:
		for address, port_string in args.coordinator:
			port = convert.try_int(port_string)
			if not port:
				print >> sys.stderr, 'Port number is not numerical.'
				exit(-1)
			coordinators.append(Coordinator(None, address, port))

	try:
		server = NodeServer(args.id, store_file=args.file, explicit_configuration=config, coordinators=coordinators)
		server.serve()
		while True:
			coio.sleep(1)
	except KeyboardInterrupt:
		pass

	print
	print 'Exiting...'

def main():
	parser = argparse.ArgumentParser(description="Tako Node")
	parser.add_argument('-id', '--id', help='Server id. Default = n1', default='n1')
	parser.add_argument('-c', '--coordinator', help='Coordinator Server (address port)', nargs=2, action='append')
	parser.add_argument('-f','--file', help='Database file.')
	parser.add_argument('-cfg','--config', help='Configuration file. For use without a coordinator.')
	parser.add_argument('-d', '--debug', help='Enable debug logging.', action='store_true')
	parser.add_argument('-p', '--profiling-file', help='Enable performance profiling.')

	try:
		args = parser.parse_args()
	except IOError, e:
		print >> sys.stderr, str(e)
		exit(-1)

	if args.profiling_file:
		import cProfile
		cProfile.runctx('_main(args)', globals(), locals(), args.profiling_file)
	else:
		_main(args)

if __name__ == '__main__':
	os.chdir(paths.home)
	main()