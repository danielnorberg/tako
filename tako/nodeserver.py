# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import urllib
import logging
import os
import email.utils

from syncless import coio
from socketless import service

import paths
paths.setup()

from utils import timestamper
from utils import debug

import httpserver
from configurationcache import ConfigurationCache
from coordinatorclient import CoordinatorClient
from protocols import InternalNodeServiceProtocol, PublicNodeServiceProtocol
from store import Store

class NoConfigurationException(BaseException):
    pass

class NodeServer(object):
    def __init__(self, node_id, store_file=None, explicit_configuration=None, coordinators=[], var_directory='var'):
        super(NodeServer, self).__init__()
        debug.log('node_id = %s, store_file = %s, explicit_configuration = %s, coordinators = %s, var_directory = %s', node_id, store_file, explicit_configuration, coordinators, var_directory)
        self.id = node_id
        self.var_directory = os.path.join(paths.home, var_directory)
        self.store_file = store_file or os.path.join(self.var_directory, 'data', '%s.tcb' % self.id)
        self.configuration_directory = os.path.join(self.var_directory, 'etc')
        self.configuration_cache = ConfigurationCache(self.configuration_directory, 'nodeserver-%s' % self.id)
        self.store = Store(self.store_file)
        self.store.open()
        self.node_clients = {}
        self.coordinators = coordinators
        self.internal_cluster_client = service.MulticastClient(InternalNodeServiceProtocol())
        self.http_handlers = (
                ('/v/', {'GET':self.store_GET, 'POST':self.store_POST}),
                # ('/i/', {'GET':self.internal_GET, 'POST':self.internal_POST}),
                # ('/s/', {'GET':self.stat_GET}),
        )
        self.coordinator_client = CoordinatorClient(coordinators=self.coordinators, callbacks=[self.evaluate_new_configuration], interval=30)

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
        neighbour_nodes = self.configuration.find_neighbour_nodes_for_node(self.node) if self.node else {}
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
        if self.id in self.configuration.active_deployment.nodes:
            self.deployment = self.configuration.active_deployment
        if self.id in self.configuration.target_deployment.nodes:
            self.deployment = self.configuration.target_deployment
        self.read_repair_enabled = self.deployment.read_repair_enabled
        self.node = self.deployment.nodes.get(self.id, None)
        # TODO: restart http server if needed
        self.http_port = self.node.http_port if self.node else None
        self.configuration_cache.cache_configuration(self.configuration)
        self.initialize_node_client_pool()

    def serve(self):
        self.coordinator_client.start()
        logging.info('Checking configuration.')
        if not self.configuration and not self.coordinators:
            logging.error('No configuration available and no coordinators configured.')
            raise NoConfigurationException()
        while not self.configuration:
            debug.log('Waiting for configuration.')
            coio.sleep(1)
        self.internal_server = service.Server(listener=(self.node.address, self.node.raw_port), services=(
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
        timestamp, value = self.store.get(key)
        callback(timestamp or 0, value)

    def internal_set(self, callback, key, timestamp, value):
        debug.log('key: %s', key)
        self.store.set(key, timestamp, value)
        callback(timestamp)

    def internal_stat(self, callback, key):
        debug.log('key: %s', key)
        timestamp, value = self.store.get(key)
        debug.log('timestamp: %s', timestamp)
        callback(timestamp or 0)

    def public_get(self, callback, key):
        debug.log('key: %s', key)
        timestamp, value = self.store.get(key)
        if self.read_repair_enabled:
            timestamp, value = self.read_repair(key, timestamp, value)
        callback(timestamp or 0, value)

    def public_set(self, callback, key, timestamp, value):
        debug.log("key: %s", key)
        target_nodes = self.configuration.find_nodes_for_key(key)
        local_node = target_nodes.pop(self.node.id, None)
        if local_node:
            local_timestamp, local_value = self.store.get(key)
            if timestamp > local_timestamp:
                debug.log('Local timestamp wins: %s > %s', local_timestamp, timestamp)
                self.store.set(key, timestamp, value)
                self.propagate(key, timestamp, value, target_nodes)
            else:
                timestamp = local_timestamp
        else:
            logging.warning('%s not in %s', self.node.id, target_nodes)
        logging.debug('timestamp: %s', timestamp)
        callback(timestamp)

    def public_stat(self, callback, key):
        def get_callback(timestamp, value):
            callback(timestamp)
        self.public_get(get_callback, key)

    def store_GET(self, start_response, path, body, env):
        debug.log('path: %s', path)
        key = self.unquote(path)
        timestamp, value = self.public_get(key)
        if timestamp and value:
            start_response('200 OK', [
                    ('Content-Type', 'application/octet-stream'),
                    ('Last-Modified', email.utils.formatdate(timestamper.to_seconds(timestamp))),
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
        self.public_set(key, timestamp, value)
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
        nodes = self.configuration.find_nodes_for_key(key)
        nodes.pop(self.node.id, None)
        clients = self.clients_for_nodes(nodes)
        timestamps = self.internal_cluster_client.stat(clients, key)
        return timestamps

    def get_timestamp(self, env):
        try:
            return timestamper.try_loads(env.get('HTTP_X_TIMESTAMP', None)) or timestamper.now()
        except ValueError:
            raise httpserver.BadRequest()

    def clients_for_nodes(self, node_ids):
        # debug.log('node_ids: %s', node_ids)
        return [self.node_clients[node_id] for node_id in node_ids]

    def propagate(self, key, timestamp, value, target_nodes):
        debug.log('key: %s, target_nodes: %s', key, target_nodes)
        collector = self.internal_cluster_client.set_collector(self.clients_for_nodes(target_nodes), 1)
        self.internal_cluster_client.set_async(collector, key, timestamp, value)

    def read_repair(self, key, timestamp, value):
        debug.log('key: %s, timestamp: %s', key, timestamp)
        remote_timestamps = self.fetch_timestamps(key)
        debug.log('remote: %s', remote_timestamps)
        newer = [(client, remote_timestamp) for client, remote_timestamp in remote_timestamps if remote_timestamp and remote_timestamp > timestamp]

        debug.log('newer: %s', newer)
        if newer:
            latest_client, latest_timestamp = newer[-1]
            latest_timestamp, latest_value = self.fetch_value(key, latest_client.tag)
            debug.log('latest_timestamp: %s', latest_timestamp)
            if latest_timestamp and latest_value:
                value = latest_value
                timestamp = latest_timestamp
                self.store.set(key, timestamp, value)

        older = [(client, remote_timestamp) for client, remote_timestamp in remote_timestamps if remote_timestamp and remote_timestamp < timestamp]
        debug.log('older: %s', older)
        if older:
            older_node_ids = [client.tag for (client, remote_timestamp) in older]
            self.propagate(key, timestamp, value, older_node_ids)

        return timestamp, value

class InternalNodeService(service.Service):
    def __init__(self, node_server):
        super(InternalNodeService, self).__init__(InternalNodeServiceProtocol(),
            get=node_server.internal_get,
            set=node_server.internal_set,
            stat=node_server.internal_stat,
        )

class PublicNodeService(service.Service):
    """docstring for PublicNodeService"""
    def __init__(self, node_server):
        super(PublicNodeService, self).__init__(PublicNodeServiceProtocol(),
            get=node_server.public_get,
            set=node_server.public_set,
            stat=node_server.public_stat,
        )