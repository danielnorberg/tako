# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import urllib
import logging
import os
import email.utils

from syncless import coio
from socketless import service

import paths
paths.setup()

from utils import debug
from utils import timestamper
from utils import httpserver

from configurationcontroller import ConfigurationController
from protocols import InternalNodeServiceProtocol, PublicNodeServiceProtocol
from store import Store

class NoConfigurationException(BaseException):
    pass

class NodeServer(object):
    def __init__(self, node_id, store_file=None, explicit_configuration=None, coordinator_addresses=[], var_directory='var',
                 configuration_update_interval=300):
        super(NodeServer, self).__init__()
        debug.log('node_id = %s, store_file = %s, explicit_configuration = %s, coordinators = %s, var_directory = %s',
                  node_id, store_file, explicit_configuration, coordinator_addresses, var_directory)
        self.id = node_id
        self.node = None
        var_directory = paths.path(var_directory)
        store_file = store_file or os.path.join(var_directory, 'data', '%s.tcb' % self.id)
        self.__store = Store(store_file)
        self.__store.open()
        self.__node_clients = {}
        self.__internal_cluster_client = service.MulticastClient(InternalNodeServiceProtocol())
        self.__http_handlers = (
                ('/values/', {'GET':self.__store_GET, 'POST':self.__store_POST}),
                # ('/internal/', {'GET':self.internal_GET, 'POST':self.internal_POST}),
                # ('/stat/', {'GET':self.stat_GET}),
        )
        configuration_directory = os.path.join(var_directory, 'etc')
        self.__configuration_controller = ConfigurationController('nodeserver-%s' % self.id, coordinator_addresses, explicit_configuration,
                                                                  configuration_directory, self.__update_configuration, configuration_update_interval)
        logging.debug('self.__configuration_controller == %s', self.__configuration_controller)

    def __initialize_node_client_pool(self):
        logging.debug('self.__configuration_controller == %s', self.__configuration_controller)
        neighbour_nodes = self.__configuration_controller.configuration.find_neighbour_nodes_for_node(self.node) if self.node else {}
        new_node_clients = {}
        for node_id, client in self.__node_clients.iteritems():
            if node_id in neighbour_nodes:
                new_node_clients[node_id] = client
            else:
                client.close()
        for node_id, node in neighbour_nodes.iteritems():
            if node_id not in new_node_clients:
                new_node_clients[node_id] = service.Client((node.address, node.raw_port), InternalNodeServiceProtocol, tag=node_id)
        self.__node_clients = new_node_clients

    def __update_configuration(self, new_configuration):
        logging.debug('self.__store == %s', self.__store)
        logging.debug('self.__configuration_controller == %s', self.__configuration_controller)
        debug.log('New configuration: %s', new_configuration)
        deployment = None
        if self.id in new_configuration.active_deployment.nodes:
            deployment = new_configuration.active_deployment
        if new_configuration.target_deployment and self.id in new_configuration.target_deployment.nodes:
            deployment = new_configuration.target_deployment
        self.read_repair_enabled = deployment.read_repair_enabled if deployment else False
        self.node = deployment.nodes.get(self.id, None) if deployment else None
        # TODO: restart http server if needed
        self.http_port = self.node.http_port if self.node else None
        self.__initialize_node_client_pool()

    def __quote(self, key):
        return urllib.quote_plus(key, safe='/&')

    def __unquote(self, path):
        return urllib.unquote_plus(path)

    def __fetch_value(self, key, node_id):
        debug.log('key: %s, node_id: %s', key, node_id)
        return self.__clients_for_nodes((node_id,))[0].get(key) or (None, None)

    def __fetch_timestamps(self, key):
        debug.log('key: %s', key)
        nodes = self.__configuration_controller.configuration.find_nodes_for_key(key)
        nodes.pop(self.node.id, None)
        if not nodes:
            return []
        clients = self.__clients_for_nodes(nodes)
        return self.__internal_cluster_client.stat(clients, key)

    def __get_timestamp(self, env):
        try:
            return timestamper.try_loads(env.get('HTTP_X_TIMESTAMP', None)) or timestamper.now()
        except ValueError:
            raise httpserver.BadRequest()

    def __clients_for_nodes(self, node_ids):
        # debug.log('node_ids: %s', node_ids)
        return [self.__node_clients[node_id] for node_id in node_ids]

    def __propagate(self, key, timestamp, value, target_nodes):
        debug.log('key: %s, target_nodes: %s', key, target_nodes)
        collector = self.__internal_cluster_client.set_collector(self.__clients_for_nodes(target_nodes), 1)
        self.__internal_cluster_client.set_async(collector, key, timestamp, value)

    def __read_repair(self, key, timestamp, value):
        debug.log('key: %s, timestamp: %s', key, timestamp)
        remote_timestamps = self.__fetch_timestamps(key)
        debug.log('remote: %s', remote_timestamps)
        newer = [(client, remote_timestamp) for client, remote_timestamp in remote_timestamps
                 if remote_timestamp and remote_timestamp > timestamp]

        debug.log('newer: %s', newer)
        if newer:
            latest_client, latest_timestamp = newer[-1]
            latest_timestamp, latest_value = self.__fetch_value(key, latest_client.tag)
            debug.log('latest_timestamp: %s', latest_timestamp)
            if latest_timestamp and latest_value:
                value = latest_value
                timestamp = latest_timestamp
                self.__store.set(key, timestamp, value)

        older = [(client, remote_timestamp) for client, remote_timestamp in remote_timestamps
                 if remote_timestamp and remote_timestamp < timestamp]
        debug.log('older: %s', older)
        if older:
            older_node_ids = [client.tag for (client, remote_timestamp) in older]
            self.__propagate(key, timestamp, value, older_node_ids)

        return timestamp, value

    def __internal_get(self, callback, key):
        debug.log('key: %s', key)
        timestamp, value = self.__store.get(key)
        callback(timestamp or 0, value)

    def __internal_set(self, callback, key, timestamp, value):
        debug.log('key: %s', key)
        self.__store.set(key, timestamp, value)
        callback(timestamp)

    def __internal_stat(self, callback, key):
        debug.log('key: %s', key)
        timestamp, value = self.__store.get(key)
        debug.log('timestamp: %s', timestamp)
        callback(timestamp or 0)

    def __public_get(self, callback, key):
        debug.log('key: %s', key)
        timestamp, value = self.__store.get(key)
        if self.read_repair_enabled:
            timestamp, value = self.__read_repair(key, timestamp, value)
        callback(timestamp or 0, value)

    def __public_set(self, callback, key, timestamp, value):
        debug.log("key: %s", key)
        target_nodes = self.__configuration_controller.configuration.find_nodes_for_key(key)
        local_node = target_nodes.pop(self.node.id, None)
        if local_node:
            local_timestamp, local_value = self.__store.get(key)
            if timestamp > local_timestamp:
                debug.log('Local timestamp loses: %s < %s', local_timestamp, timestamp)
                self.__store.set(key, timestamp, value)
                self.__propagate(key, timestamp, value, target_nodes)
            else:
                debug.log('Local timestamp wins: %s > %s', local_timestamp, timestamp)
                timestamp = local_timestamp
        else:
            logging.warning('%s not in %s', self.node.id, target_nodes)
        logging.debug('timestamp: %s', timestamp)
        callback(timestamp)

    def __public_stat(self, callback, key):
        def __get_callback(timestamp, value):
            callback(timestamp)
        self.__public_get(__get_callback, key)

    def __store_GET(self, start_response, path, body, env):
        debug.log('path: %s', path)
        key = self.__unquote(path)
        timestamp, value = self.__public_get(key)
        if timestamp and value:
            start_response('200 OK', [
                    ('Content-Type', 'application/octet-stream'),
                    ('Last-Modified', email.utils.formatdate(timestamper.to_seconds(timestamp))),
                    ('X-Timestamp', str(timestamp)),
            ])
            return [value]
        else:
            start_response('404 Not Found', [])
            return ['']

    def __store_POST(self, start_response, path, body, env):
        debug.log("path: %s", path)
        key = self.__unquote(path)
        value = body.read()
        timestamp = self.__get_timestamp(env)
        self.public_set(key, timestamp, value)
        start_response('200 OK', [
                ('Content-Type', 'application/octet-stream'),
                ('X-Timestamp', str(timestamp)),
        ])
        return ['']

    def serve(self):
        self.__configuration_controller.start()
        while not self.node:
            debug.log('Waiting for configuration.')
            coio.sleep(1)

        internal_service = service.Service(
            InternalNodeServiceProtocol(),
            get=self.__internal_get,
            set=self.__internal_set,
            stat=self.__internal_stat,
        )

        public_service = service.Service(
            PublicNodeServiceProtocol(),
            get=self.__public_get,
            set=self.__public_set,
            stat=self.__public_stat,
        )

        logging.info('Internal API: %s:%s' % (self.node.address, self.node.raw_port))
        self.internal_server = service.Server(listener=(self.node.address, self.node.raw_port),
                                              services=(internal_service, public_service))
        self.internal_server.serve()

        logging.info('Public HTTP API: %s:%s' % (self.node.address, self.node.http_port))
        self.http_server = httpserver.HttpServer(listener=(self.node.address, self.node.http_port),
                                                 handlers=self.__http_handlers)
        self.http_server.serve()
