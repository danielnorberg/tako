# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-
import struct

import paths
paths.setup()

from socketless import service

from utils import debug
from coordinatorclient import CoordinatorClient
from configurationcache import ConfigurationCache
from protocols import PublicNodeServiceProtocol
from models import Coordinator

def timestamped_value(timestamp, value):
    return struct.pack('Q', timestamp) + value

class Client(object):
    def __init__(self, coordinator_addresses=[], explicit_configuration=None, configuration_cache_directory=None):
        super(Client, self).__init__()
        coordinators = [Coordinator(None, address, port) for address, port in coordinator_addresses]
        self.coordinator_client = CoordinatorClient(coordinators=coordinators, callbacks=[self.evaluate_new_configuration], interval=30)
        self.configuration_cache = None
        self.node_clients = dict()

        if configuration_cache_directory:
            debug.log('Persistent configuration cache enabled. (%s)', configuration_cache_directory)
            self.configuration_cache = ConfigurationCache(self.configuration_cache_directory, 'nodeserver-%s' % self.id)
        else:
            debug.log('No configuration cache directory., persistent configuration cache disabled.')

        self.configuration = None
        if explicit_configuration:
            debug.log('Using explicit configuration')
            self.evaluate_new_configuration(explicit_configuration)
        elif self.configuration_cache:
            cached_configuration = self.configuration_cache.get_configuration()
            if cached_configuration:
                debug.log('Using cached configuration')
                self.evaluate_new_configuration(cached_configuration)

        self.coordinator_client.start()

        if not self.configuration and not coordinator_addresses:
            raise Exception('No configuration available.')

    def evaluate_new_configuration(self, new_configuration):
        if not self.configuration or new_configuration.timestamp > self.configuration.timestamp:
            self.set_configuration(new_configuration)

    def initialize_node_client_pool(self):
        nodes = self.configuration.all_nodes()
        new_node_clients = {}
        for node_id, client in self.node_clients.iteritems():
            if node_id in nodes.iteritems():
                new_node_clients[node_id] = client
            else:
                client.close()
        for node_id, node in nodes.iteritems():
            if node_id not in new_node_clients:
                new_node_clients[node_id] = service.Client((node.address, node.raw_port), PublicNodeServiceProtocol, tag=node_id)
        self.node_clients = new_node_clients

    def set_configuration(self, new_configuration):
        debug.log(new_configuration)
        self.configuration = new_configuration
        if self.configuration_cache:
            self.configuration_cache.cache_configuration(self.configuration)
        self.initialize_node_client_pool()

    def connected_node_count(self):
        return len([node_client for node_client in self.node_clients.itervalues() if node_client.is_connected()])

    def total_node_count(self):
        return len(self.node_clients)

    def has_configuration(self):
        return self.configuration != None

    def is_connected(self):
        return self.has_configuration() and self.connected_node_count() > self.total_node_count() / 2.0

    def client_for_key(self, key):
        # TODO: Distribute node selection evenly?
        nodes = self.configuration.find_nodes_for_key(key)
        for node in nodes.itervalues():
            client = self.node_clients[node.id]
            if client.is_connected():
                return client
        return None

    def set_value(self, key, value, timestamp):
        node_client = self.client_for_key(key)
        if not node_client:
            raise Exception('No node available for key: %s', key)
        node_client.set(key, timestamped_value(timestamp, value))
        # node_client.set(timestamp, key, value)

    def get_value(self, key):
        """docstring for get"""
        pass
