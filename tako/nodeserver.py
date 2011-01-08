# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import urllib
import logging
import os
import email.utils
import struct

from syncless import coio
from syncless.util import Queue

from socketless.messenger import Messenger, invoke_all
from socketless.channelserver import ChannelServer
from socketless.channel import DisconnectedException

from utils.timestamp import Timestamp
# from utils import convert
from utils import debug

import httpserver
import paths
from store import Store
# import configuration
from configurationcache import ConfigurationCache
# from configuration import Coordinator
# from coordinatorclient import CoordinatorClient

class NoConfigurationException(BaseException):
    pass

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
    ERROR = 'E'

# INTERNAL_HANDSHAKE = ('Tako Internal API', 'K')
# PUBLIC_HANDSHAKE = ('Tako Public API', 'K')

class ServiceServer(object):
    """docstring for ServiceServer"""
    def __init__(self, listener, services):
        super(ServiceServer, self).__init__()
        self.services = dict((service.handshake[0], service) for service in services)
        self.listener = listener
        self.channel_server = ChannelServer(self.listener, handle_connection=self.handle_connection)

    def handshake(self, channel):
        debug.log('Awaiting challenge.')
        challenge = channel.recv()
        debug.log('Got challenge: "%s"', challenge)
        service = self.services.get(challenge, None)
        if not service:
            logging.warning('Failed handshake!')
            channel.send(Responses.ERROR)
            return None
        debug.log('Correct challenge, sending response: "%s"', service.handshake[1])
        channel.send(service.handshake[1])
        channel.flush()
        debug.log('Succesfully completed handshake.')
        return service

    def handle_connection(self, channel, addr):
        try:
            service = self.handshake(channel)
            service.handle_connection(channel)
        except DisconnectedException:
            logging.info('client %s disconnected', addr)
        except BaseException, e:
            logging.exception(e)
        finally:
            try:
                channel.close()
            except DisconnectedException, e:
                pass

    def serve(self):
        logging.info("Listening on %s", self.listener)
        self.channel_server.serve()


class Service(object):
    """docstring for Service"""
    def __init__(self, handshake, handlers):
        super(Service, self).__init__()
        self.handshake = handshake
        self.handlers = handlers

    def _flush_loop(self, channel, flush_queue):
        try:
            while True:
                flush_queue.popleft()
                channel.flush()
        except DisconnectedException:
            pass

    def handle_connection(self, channel):
        flush_queue = Queue()
        flusher = coio.stackless.tasklet(self._flush_loop)(channel, flush_queue)
        try:
            while True:
                message = channel.recv()
                if not message:
                    debug.log('Channel closing.')
                    break
                reader = MessageReader(message)
                request = reader.read(1)
                handler = self.handlers.get(request, None)
                if not handler:
                    channel.send(Responses.ERROR)
                    return
                response = handler(reader)
                channel.send(response)
                if len(flush_queue) == 0:
                    flush_queue.append(True)
        finally:
            flusher.kill()

class InternalNodeService(Service):
    def __init__(self, node_server):
        super(InternalNodeService, self).__init__(('Tako Internal API', 'K'), {
            'G': self.get,
            'S': self.set,
            'T': self.stat,
        })
        self.node_server = node_server

    def get(self, arguments):
        key_length = arguments.read_int()
        key = arguments.read(key_length)
        debug.log('key: %s', key)
        timestamped_value = self.node_server.store.get_timestamped(key)
        if timestamped_value:
            return (Responses.OK, timestamped_value)
        else:
            return Responses.NOT_FOUND

    def set(self, arguments):
        key_length = arguments.read_int()
        value_length = arguments.read_int()
        key = arguments.read(key_length)
        debug.log('key: %s', key)
        value = arguments.read(value_length)
        self.node_server.store.set_timestamped(key, value)
        return Responses.OK

    def stat(self, arguments):
        key_length = arguments.read_int()
        key = arguments.read(key_length)
        debug.log('key: %s', key)
        value, timestamp = self.node_server.store.get(key)
        if timestamp:
            return (Responses.OK, struct.pack('!Q', timestamp.microseconds))
        else:
            return Responses.NOT_FOUND

class PublicNodeService(Service):
    """docstring for PublicNodeService"""
    def __init__(self, node_server):
        super(PublicNodeService, self).__init__(('Tako Public API', 'K'), {
            'G': self.get,
            'S': self.set,
            # 'T': self.stat,
        })
        self.node_server = node_server

    def get(self, arguments):
          key_length = arguments.read_int()
          key = arguments.read(key_length)
          debug.log('key: %s', key)
          timestamped_value = self.node_server.get_value(key)
          if timestamped_value:
              return (Responses.OK, timestamped_value)
          else:
              return Responses.NOT_FOUND

    def set(self, arguments):
        key_length = arguments.read_int()
        value_length = arguments.read_int()
        key = arguments.read(key_length)
        debug.log('key: %s', key)
        timestamped_value = arguments.read(value_length)
        self.node_server.set_value(key, timestamped_value)
        return Responses.OK

class NodeServer(object):
    def __init__(self, node_id, store_file=None, explicit_configuration=None, coordinators=[], var_directory='var'):
        super(NodeServer, self).__init__()
        self.GET_VALUE = Requests.GET_VALUE
        self.SET_VALUE = Requests.SET_VALUE
        self.GET_TIMESTAMP = Requests.GET_TIMESTAMP

        self.id = node_id
        self.var_directory = os.path.join(paths.home, var_directory)
        self.store_file = store_file or os.path.join(self.var_directory, 'data', '%s.tcb' % self.id)
        self.configuration_directory = os.path.join(self.var_directory, 'etc')
        self.configuration_cache = ConfigurationCache(self.configuration_directory, 'nodeserver-%s' % self.id)
        self.store = Store(self.store_file)
        self.store.open()
        self.node_messengers = {}
        self.coordinators = coordinators
        self.http_handlers = (
                ('/store/', {'GET':self.store_GET, 'POST':self.store_POST}),
                # ('/internal/', {'GET':self.internal_GET, 'POST':self.internal_POST}),
                # ('/stat/', {'GET':self.stat_GET}),
        )
        # self.coordinator_client = CoordinatorClient(coordinators=self.coordinators, callbacks=[self.evaluate_new_configuration], interval=30)

        self.configuration = None
        if explicit_configuration:
            debug.log('using explicit configuration')
            self.evaluate_new_configuration(explicit_configuration)
        else:
            cached_configuration = self.configuration_cache.get_configuration()
            if cached_configuration:
                debug.log('using cached configuration')
                self.evaluate_new_configuration(cached_configuration)

    def evaluate_new_configuration(self, new_configuration):
        """docstring for evaluate_new_configuration"""
        if not self.configuration or new_configuration.timestamp > self.configuration.timestamp:
            self.set_configuration(new_configuration)

    def initialize_messenger_pool(self):
        neighbour_nodes = self.configuration.find_neighbour_nodes_for_node(self.node)
        new_node_messengers = {}
        for node_id, messenger in self.node_messengers.iteritems():
            if node_id in neighbour_nodes.iteritems():
                new_node_messengers[node_id] = messenger
            else:
                messenger.close()
        for node_id, node in neighbour_nodes.iteritems():
            if node_id not in new_node_messengers:
                new_node_messengers[node_id] = Messenger((node.address, node.raw_port), handshake=('Tako Internal API', 'K'))
        self.node_messengers = new_node_messengers

    def set_configuration(self, new_configuration):
        """docstring for configuration"""
        debug.log(new_configuration)
        self.configuration = new_configuration
        self.deployment = self.configuration.active_deployment
        self.read_repair_enabled = self.configuration.active_deployment.read_repair_enabled
        self.node = self.deployment.nodes[self.id]
        self.http_port = self.node.http_port
        self.configuration_cache.cache_configuration(self.configuration)
        self.initialize_messenger_pool()

    def serve(self):
        """docstring for server"""
        # self.coordinator_client.start()
        logging.info('Checking Configuration.')
        if not self.configuration and not self.coordinators:
            logging.critical('Missing Configuration!')
            raise NoConfigurationException()
        while not self.configuration:
            debug.log('Waiting for configuration.')
            coio.sleep(1)
        self.internal_server = ServiceServer(listener=(self.node.address, self.node.raw_port), services=(
            InternalNodeService(self),
            PublicNodeService(self),
        ))
        self.internal_server.serve()
        self.http_server = httpserver.HttpServer(listener=(self.node.address, self.node.http_port), handlers=self.http_handlers)
        self.http_server.serve()

    def request_message(self, request, key=None, value=None):
        """docstring for message"""
        if not key:
            return request
        else:
            if value:
                fragments = (struct.pack('!cLL', request, len(key), len(value)), str(key), str(value))
            else:
                fragments = (struct.pack('!cL', request, len(key)), str(key))
            return ''.join(fragments)

    def quote(self, key):
        """docstring for quote"""
        return urllib.quote_plus(key, safe='/&')

    def unquote(self, path):
        """docstring for unquote"""
        return urllib.unquote_plus(path)

    def get_value(self, key):
        """docstring for get_value"""
        debug.log('key: %s', key)
        timestamped_value = self.store.get_timestamped(key)
        if self.read_repair_enabled:
            timestamped_value = self.read_repair(key, timestamped_value)
        return timestamped_value

    def set_value(self, key, timestamped_value):
        debug.log("key: %s", key)
        self.store.set_timestamped(key, timestamped_value)
        target_nodes = self.configuration.find_neighbour_nodes_for_key(key, self.node)
        self.propagate(key, timestamped_value, target_nodes.values())

    def store_GET(self, start_response, path, body, env):
        debug.log('path: %s', path)
        key = self.unquote(path)
        timestamped_value = self.get_value(key)
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
        debug.log("path: %s", path)
        key = self.unquote(path)
        value = body.read()
        timestamp = self.get_timestamp(env)
        timestamped_value = self.store.pack_timestamped_data(value, timestamp)
        self.set_value(key, timestamped_value)
        start_response('200 OK', [
                ('Content-Type', 'application/octet-stream'),
                ('X-TimeStamp', str(timestamp)),
        ])
        return ['']

    def fetch_value(self, key, node):
        """docstring for fetch_value"""
        debug.log('key: %s, node: %s', key, node)
        messengers = self.messengers_for_nodes([node])
        message = self.request_message(Requests.GET_VALUE, key)
        [reply] = invoke_all(message, messengers)
        reply = MessageReader(reply)
        if reply.read(1) == Responses.OK:
            return reply.read()
        else:
            return None

    def fetch_timestamps(self, key):
        """docstring for fetch_timestamps"""
        debug.log('key: %s', key)
        neighbour_nodes = self.configuration.find_neighbour_nodes_for_key(key, self.node)
        messengers = self.messengers_for_nodes(neighbour_nodes.values())
        message = self.request_message(Requests.GET_TIMESTAMP, key)
        replies = invoke_all(message, messengers)
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
        return [(node, self.node_messengers[node.id]) for node in nodes]

    def null_callback(self, value, token):
        pass

    def propagate(self, key, timestamped_value, target_nodes):
        """docstring for propagate"""
        # # debug.log('key: %s', key)
        # if not target_nodes:
        #     target_nodes =
        debug.log('target_nodes: %s', target_nodes)
        # if not target_nodes:
        #     return
        message = self.request_message(self.SET_VALUE, key, timestamped_value)
        for node in target_nodes:
            messenger = self.node_messengers[node.id]
            messenger.send(message, node, self.null_callback)

        # message = self.request_message(Requests.SET_VALUE, key, timestamped_value)
        # messengers = self.messengers_for_nodes(target_nodes.values())
        # debug.log('messengers: %s', messengers)
        # for node, messenger in messengers:
        #     messenger.send(message, node, self.null_callback)

    def read_repair(self, key, timestamped_value):
        """docstring for read_repair"""
        timestamp = self.store.read_timestamp(timestamped_value) if timestamped_value else None
        debug.log('key: %s, timestamp: %s', key, timestamp)
        remote_timestamps = self.fetch_timestamps(key)
        debug.log('remote: %s', remote_timestamps)
        newer = [(remote_timestamp, node) for remote_timestamp, node in remote_timestamps if remote_timestamp > timestamp]

        debug.log('newer: %s', newer)
        if newer:
            latest_timestamp, latest_node = newer[-1]
            latest_timestamped_value = self.fetch_value(key, latest_node)
            if latest_timestamped_value:
                timestamped_value = latest_timestamped_value
                timestamp = self.store.read_timestamp(latest_timestamped_value)
                self.store.set_timestamped(key, latest_timestamped_value)

        older = [(remote_timestamp, node) for remote_timestamp, node in remote_timestamps if remote_timestamp < timestamp]
        debug.log('older: %s', older)
        if older:
            older_nodes = [node for (remote_timestamp, node) in older]
            self.propagate(key, timestamped_value, older_nodes)

        return timestamped_value
