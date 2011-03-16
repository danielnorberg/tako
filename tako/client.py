# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-
import logging

import paths
paths.setup()

from socketless import service

from utils import debug
from configurationcontroller import ConfigurationController
from protocols import PublicNodeServiceProtocol

class ValueNotAvailableException(BaseException):
    def __init__(self, key):
        super(ValueNotAvailableException, self).__init__('Value not currently available for key "%s"' % repr(key))
        self.key = key

class Client(object):
    def __init__(self, name, coordinator_addresses=[], explicit_configuration=None, configuration_cache_directory=None,
                 configuration_update_interval=5*30):
        super(Client, self).__init__()
        self.__node_clients = {}
        self.__configuration = None
        self.__configuration_controller = ConfigurationController(name, coordinator_addresses, explicit_configuration,
                                                                  configuration_cache_directory, self.__update_configuration,
                                                                  configuration_update_interval)

    def __initialize_node_client_pool(self):
        new_nodes = self.__configuration.all_nodes()
        new_node_clients = {}
        for node_id, client in self.__node_clients.iteritems():
            if node_id in new_nodes:
                new_node_clients[node_id] = client
            else:
                client.close()
        for node_id, node in new_nodes.iteritems():
            if node_id not in new_node_clients:
                new_node_clients[node_id] = service.Client((node.address, node.raw_port), PublicNodeServiceProtocol, tag=node_id)
        self.__node_clients = new_node_clients

    def __update_configuration(self, new_configuration):
        debug.log(new_configuration)
        self.__configuration = new_configuration
        self.__initialize_node_client_pool()

    def __client_for_key(self, key):
        # TODO: Distribute node selection evenly?
        if not self.__configuration:
            return None
        nodes = self.__configuration.find_nodes_for_key(key)
        for node in nodes.itervalues():
            client = self.__node_clients[node.id]
            if client.is_connected():
                return client
        return None

    def connect(self):
        self.__configuration_controller.start()

    def disconnect(self):
        raise Exception('Not implemented')

    def connected_node_count(self):
        return len([node_client for node_client in self.__node_clients.itervalues() if node_client.is_connected()])

    def total_node_count(self):
        return len(self.__node_clients)

    def has_configuration(self):
        return self.__configuration_controller.configuration != None

    def is_connected(self):
        return self.has_configuration() and self.connected_node_count() > self.total_node_count() / 2.0

    def set(self, key, timestamp, value):
        logging.debug('key = %s, timestamp = %s', key, timestamp)
        node_client = self.__client_for_key(key)
        if not node_client:
            raise ValueNotAvailableException(key)
        return node_client.set(key, timestamp, value)

    def get(self, key):
        node_client = self.__client_for_key(key)
        if not node_client:
            raise ValueNotAvailableException(key)
        result = node_client.get(key)
        if not result:
            raise ValueNotAvailableException(key)
        timestamp, value = result
        return timestamp or None, value

    def stat(self, key):
        node_client = self.__client_for_key(key)
        if not node_client:
            raise ValueNotAvailableException(key)
        return node_client.stat(key) or None
