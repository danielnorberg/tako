# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-
import struct
import logging

import paths
paths.setup()

from socketless import service

from utils import debug
from coordinatorclient import CoordinatorClient
from configurationcache import ConfigurationCache
from protocols import PublicNodeServiceProtocol
from models import Coordinator

class ValueNotAvailableException(BaseException):
    def __init__(self, key):
        super(ValueNotAvailableException, self).__init__('Value not currently available for key "%s"' % repr(key))
        self.key = key


class Client(object):
    def __init__(self, coordinator_addresses=[], explicit_configuration=None, configuration_cache_directory=None):
        super(Client, self).__init__()
        coordinators = [Coordinator(None, address, port) for address, port in coordinator_addresses]
        self.coordinator_client = CoordinatorClient(coordinators=coordinators, callbacks=[self.__evaluate_new_configuration], interval=30)
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
            self.__evaluate_new_configuration(explicit_configuration)
        elif self.configuration_cache:
            cached_configuration = self.configuration_cache.get_configuration()
            if cached_configuration:
                debug.log('Using cached configuration')
                self.__evaluate_new_configuration(cached_configuration)

        self.coordinator_client.start()

        if not self.configuration and not coordinator_addresses:
            raise Exception('No configuration available.')

    def __evaluate_new_configuration(self, new_configuration):
        if not self.configuration or new_configuration.timestamp > self.configuration.timestamp:
            self.__set_configuration(new_configuration)

    def __initialize_node_client_pool(self):
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

    def __set_configuration(self, new_configuration):
        debug.log(new_configuration)
        self.configuration = new_configuration
        if self.configuration_cache:
            self.configuration_cache.cache_configuration(self.configuration)
        self.__initialize_node_client_pool()

    def __client_for_key(self, key):
        # TODO: Distribute node selection evenly?
        nodes = self.configuration.find_nodes_for_key(key)
        for node in nodes.itervalues():
            client = self.node_clients[node.id]
            if client.is_connected():
                return client
        return None

    def connected_node_count(self):
        return len([node_client for node_client in self.node_clients.itervalues() if node_client.is_connected()])

    def total_node_count(self):
        return len(self.node_clients)

    def has_configuration(self):
        return self.configuration != None

    def is_connected(self):
        return self.has_configuration() and self.connected_node_count() > self.total_node_count() / 2.0

    def set_value(self, key, timestamp, value):
        logging.debug('key = %s, timestamp = %s', key, timestamp)
        node_client = self.__client_for_key(key)
        if not node_client:
            raise ValueNotAvailableException(key)
        return node_client.set(key, timestamp, value)

    def get_value(self, key):
        """docstring for get"""
        node_client = self.__client_for_key(key)
        if not node_client:
            raise ValueNotAvailableException(key)
        return node_client.get(key)
