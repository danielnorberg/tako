# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-
import logging

import paths
paths.setup()

from socketless import service
from syncless import coio

from configurationcontroller import ConfigurationController
from protocols import PublicNodeServiceProtocol

class NotAvailableException(BaseException):
    def __init__(self, key):
        super(NotAvailableException, self).__init__('Value not currently available for key "%s"' % repr(key))
        self.key = key

class Client(object):
    def __init__(self, name, coordinator_addresses=[], explicit_configuration=None, configuration_cache_directory=None,
                 configuration_update_interval=5*30, max_retries=30, retry_interval=1.0):
        super(Client, self).__init__()
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.__node_clients = {}
        self.__configuration = None
        self.__configuration_controller = ConfigurationController(name, coordinator_addresses, explicit_configuration,
                                                                  configuration_cache_directory, self.__update_configuration,
                                                                  configuration_update_interval)

    def __initialize_node_client_pool(self):
        nodes = self.__configuration.all_nodes()

        recycled_node_clients = {}
        for node_id, client in self.__node_clients.iteritems():
            if node_id in nodes:
                recycled_node_clients[node_id] = client
            else:
                client.disconnect()

        new_node_clients = {}
        for node_id, node in nodes.iteritems():
            if node_id not in recycled_node_clients:
                new_node_clients[node_id] = service.Client((node.address, node.port), PublicNodeServiceProtocol, tag=node_id)

        self.__node_clients = dict(recycled_node_clients, **new_node_clients)

        # Blocking, do this after setting self.__node_clients
        for client in new_node_clients.itervalues():
            client.connect()

    def __update_configuration(self, new_configuration):
        if __debug__: logging.debug(new_configuration)
        self.__configuration = new_configuration
        self.__initialize_node_client_pool()

    def __connected_clients_for_key(self, key):
        # TODO: Distribute node selection evenly?
        if not self.__node_clients or not self.__configuration:
            return []
        nodes = self.__configuration.find_nodes_for_key(key)
        clients = [self.__node_clients[node_id] for node_id in nodes.keys()]
        connected_clients = [client for client in clients if client.is_connected()]
        if __debug__: logging.debug('connected_clients = %s', connected_clients)
        return connected_clients

    def connect(self):
        self.__configuration_controller.start()

    def disconnect(self):
        self.__configuration_controller.stop()
        for node_id, client in self.__node_clients.iteritems():
            client.disconnect()
        self.__node_clients = None

    def connected_node_count(self):
        return len([node_client for node_client in self.__node_clients.itervalues() if node_client.is_connected()])

    def total_node_count(self):
        return len(self.__node_clients)

    def has_configuration(self):
        return self.__configuration_controller.configuration != None

    def is_connected(self, complete=False):
        if not complete:
            return self.has_configuration() and self.connected_node_count() > self.total_node_count() / 2.0
        else:
            return self.has_configuration() and self.connected_node_count() == self.total_node_count();

    def set(self, key, timestamp, value):
        if __debug__: logging.debug('key = %s, timestamp = %s', key, timestamp)
        for i in xrange(self.max_retries + 1):
            node_clients = self.__connected_clients_for_key(key)
            for node_client in node_clients:
                new_timestamp = node_client.set(key, timestamp, value)
                if new_timestamp is not None:
                    return new_timestamp
            coio.sleep(self.retry_interval)
        raise NotAvailableException(key)

    def get(self, key):
        if __debug__: logging.debug('key = %s', key)
        for i in xrange(self.max_retries + 1):
            node_clients = self.__connected_clients_for_key(key)
            for node_client in node_clients:
                result = node_client.get(key)
                if __debug__: logging.debug('result = %s', result)
                if result is not None:
                    timestamp, value = result
                    return timestamp or None, value
            coio.sleep(self.retry_interval)
        raise NotAvailableException(key)

    def stat(self, key):
        if __debug__: logging.debug('key = %s', key)
        for i in xrange(self.max_retries + 1):
            node_clients = self.__connected_clients_for_key(key)
            for node_client in node_clients:
                timestamp = node_client.stat(key)
                if timestamp is not None:
                    return timestamp
            coio.sleep(self.retry_interval)
        raise NotAvailableException(key)
