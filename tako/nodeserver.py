# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import urllib
import logging
import os
import email.utils

from syncless import coio
from socketless import service

from utils.timestamp import Timestamp
from utils import debug

import httpserver
import paths
from store import Store
# import configuration
from configurationcache import ConfigurationCache
# from configuration import Coordinator
# from coordinatorclient import CoordinatorClient

from socketless.service import Service, Server, Protocol, Method

class NoConfigurationException(BaseException):
    pass

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
        self.node_clients = {}
        self.coordinators = coordinators
        self.internal_multi_client = service.MulticastClient(InternalNodeServiceProtocol())
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
        if not self.configuration or new_configuration.timestamp > self.configuration.timestamp:
            self.set_configuration(new_configuration)

    def initialize_node_client_pool(self):
        neighbour_nodes = self.configuration.find_neighbour_nodes_for_node(self.node)
        new_node_clients = {}
        for node_id, client in self.node_clients.iteritems():
            if node_id in neighbour_nodes.iteritems():
                new_node_clients[node_id] = client
            else:
                client.close()
        for node_id, node in neighbour_nodes.iteritems():
            if node_id not in new_node_clients:
                new_node_clients[node_id] = service.Client((node.address, node.raw_port), InternalNodeServiceProtocol, tag=node_id)
        self.node_clients = new_node_clients

    def set_configuration(self, new_configuration):
        debug.log(new_configuration)
        self.configuration = new_configuration
        self.deployment = self.configuration.active_deployment
        self.read_repair_enabled = self.configuration.active_deployment.read_repair_enabled
        self.node = self.deployment.nodes[self.id]
        self.http_port = self.node.http_port
        self.configuration_cache.cache_configuration(self.configuration)
        self.initialize_node_client_pool()

    def serve(self):
        # self.coordinator_client.start()
        logging.info('Checking Configuration.')
        if not self.configuration and not self.coordinators:
            logging.critical('Missing Configuration!')
            raise NoConfigurationException()
        while not self.configuration:
            debug.log('Waiting for configuration.')
            coio.sleep(1)
        self.internal_server = Server(listener=(self.node.address, self.node.raw_port), services=(
            InternalNodeService(self),
            PublicNodeService(self),
        ))
        self.http_server = httpserver.HttpServer(listener=(self.node.address, self.node.http_port), handlers=self.http_handlers)
        logging.info('Internal API: %s:%s' % (self.node.address, self.node.raw_port))
        logging.info('Public HTTP API: %s:%s' % (self.node.address, self.node.http_port))
        self.internal_server.serve()
        self.http_server.serve()

    def quote(self, key):
        return urllib.quote_plus(key, safe='/&')

    def unquote(self, path):
        return urllib.unquote_plus(path)

    def internal_get(self, callback, key):
        debug.log('key: %s', key)
        callback(self.store.get_timestamped(key))

    def internal_set(self, callback, key, timestamped_value):
        debug.log('key: %s', key)
        self.store.set(key, timestamped_value)
        callback()

    def internal_stat(self, callback, key):
        debug.log('key: %s', key)
        timestamp = self.store.get_timestamp(key) or 0
        debug.log('timestamp: %s', timestamp)
        callback(timestamp)

    def public_get(self, callback, key):
        debug.log('key: %s', key)
        timestamped_value = self.store.get_timestamped(key)
        if self.read_repair_enabled:
            timestamped_value = self.read_repair(key, timestamped_value)
        callback(timestamped_value)

    def public_set(self, callback, key, timestamped_value):
        debug.log("key: %s", key)
        self.store.set_timestamped(key, timestamped_value)
        target_nodes = self.configuration.find_neighbour_nodes_for_key(key, self.node)
        self.propagate(key, timestamped_value, target_nodes)
        callback()

    def public_stat(self, callback, key):
        value = self.store.get(key)
        callback(value[1] if value else None)

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

    def fetch_value(self, key, node_id):
        debug.log('key: %s, node_id: %s', key, node_id)
        return self.clients_for_nodes((node_id,))[0].get(key)

    def fetch_timestamps(self, key):
        debug.log('key: %s', key)
        neighbour_nodes = self.configuration.find_neighbour_nodes_for_key(key, self.node)
        clients = self.clients_for_nodes(neighbour_nodes)
        timestamps = self.internal_multi_client.stat(clients, key)
        return timestamps

    def get_timestamp(self, env):
        try:
            return Timestamp.try_loads(env.get('HTTP_X_TIMESTAMP', None)) or Timestamp.now()
        except ValueError:
            raise httpserver.BadRequest()

    def clients_for_nodes(self, node_ids):
        # debug.log('node_ids: %s', node_ids)
        return [self.node_clients[node_id] for node_id in node_ids]

    def propagate(self, key, timestamped_value, target_nodes):
        debug.log('key: %s, target_nodes: %s', key, target_nodes)
        collector = self.internal_multi_client.set_collector(self.clients_for_nodes(target_nodes), 1)
        self.internal_multi_client.set_async(collector, key, timestamped_value)

    def read_repair(self, key, timestamped_value):
        timestamp = self.store.read_timestamp(timestamped_value) if timestamped_value else None
        debug.log('key: %s, timestamp: %s', key, timestamp)
        remote_timestamps = self.fetch_timestamps(key)
        debug.log('remote: %s', remote_timestamps)
        newer = [(client, remote_timestamp) for client, remote_timestamp in remote_timestamps if remote_timestamp and remote_timestamp > timestamp]

        debug.log('newer: %s', newer)
        if newer:
            latest_client, latest_timestamp = newer[-1]
            latest_timestamped_value = self.fetch_value(key, latest_client.tag)
            debug.log('latest_timestamped_value: %s', latest_timestamped_value)
            if latest_timestamped_value:
                timestamped_value = latest_timestamped_value
                timestamp = self.store.read_timestamp(latest_timestamped_value)
                self.store.set_timestamped(key, latest_timestamped_value)

        older = [(client, remote_timestamp) for client, remote_timestamp in remote_timestamps if remote_timestamp and remote_timestamp < timestamp]
        debug.log('older: %s', older)
        if older:
            older_node_ids = [client.tag for (client, remote_timestamp) in older]
            self.propagate(key, timestamped_value, older_node_ids)

        return timestamped_value

class InternalNodeServiceProtocol(Protocol):
    handshake = ('Tako Internal Node API Service', 'Tako Internal Node API Client')
    methods = dict(
        get  = Method('g', [('key', str)], [('value', str)]), # key -> value
        set  = Method('s', [('key', str), ('value', str)], []), # key, value -> None
        stat = Method('t', [('key', str)], [('timestamp', long)]), # key -> timestamp
    )

class PublicNodeServiceProtocol(Protocol):
    handshake = ('Tako Public Node API Service', 'Tako Public Node API Client')
    methods = dict(
        get = Method('g', [('key', str)], [('value', str)]), # key -> value
        set = Method('s', [('key', str), ('value', str)], []), # key, value -> None
        stat = Method('t', [('key', str)], [('timestamp', long)]), # key -> timestamp
    )

class InternalNodeService(Service):
    def __init__(self, node_server):
        super(InternalNodeService, self).__init__(InternalNodeServiceProtocol(),
            get=node_server.internal_get,
            set=node_server.internal_set,
            stat=node_server.internal_stat,
        )

class PublicNodeService(Service):
    """docstring for PublicNodeService"""
    def __init__(self, node_server):
        super(PublicNodeService, self).__init__(PublicNodeServiceProtocol(),
            get=node_server.public_get,
            set=node_server.public_set,
            stat=node_server.public_stat,
        )