# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from datetime import timedelta
import logging
import os
import time
import urllib

from syncless import coio
from socketless import service

import paths
paths.setup()

from configurationcontroller import ConfigurationController
from protocols import InternalNodeServiceProtocol, PublicNodeServiceProtocol
from store import Store

class NoConfigurationException(BaseException):
    pass

class NodeServer(object):
    def __init__(self, node_id, store_file=None, explicit_configuration=None, coordinator_addresses=[], var_directory='var',
                 configuration_update_interval=300):
        super(NodeServer, self).__init__()
        if __debug__: logging.debug('node_id = %s, store_file = %s, explicit_configuration = %s, coordinators = %s, var_directory = %s',
                  node_id, store_file, explicit_configuration, coordinator_addresses, var_directory)
        self.id = node_id
        self.node = None
        var_directory = paths.path(var_directory)
        store_file = store_file or os.path.join(var_directory, 'data', '%s.tcb' % self.id)
        self.__store = Store(store_file)
        self.__store.open()
        self.__node_clients = {}
        self.__internal_cluster_client = service.MulticastClient(InternalNodeServiceProtocol())
        configuration_directory = os.path.join(var_directory, 'etc')
        self.__background_repair_enabled = False
        self.__background_repair_cycle_length_seconds = None
        self.__read_repair_enabled = False
        self.__configuration = None
        self.__configuration_controller = ConfigurationController('nodeserver-%s' % self.id,
                                                                  coordinator_addresses, explicit_configuration,
                                                                  configuration_directory, self.__update_configuration,
                                                                  configuration_update_interval)
        if __debug__: logging.debug('self.__configuration_controller == %s', self.__configuration_controller)

    def __initialize_node_client_pool(self):
        if __debug__: logging.debug('self.__configuration == %s', self.__configuration)
        neighbour_nodes = self.__configuration.find_neighbour_nodes_for_node(self.node) if self.node else {}
        new_node_clients = {}
        for node_id, client in self.__node_clients.iteritems():
            if node_id in neighbour_nodes:
                new_node_clients[node_id] = client
            else:
                client.close()
        for node_id, node in neighbour_nodes.iteritems():
            if node_id not in new_node_clients:
                new_node_clients[node_id] = service.Client((node.address, node.port), InternalNodeServiceProtocol, tag=node_id)
        self.__node_clients = new_node_clients

    def __update_configuration(self, new_configuration):
        if __debug__: logging.debug('New configuration: %s', new_configuration)
        self.__configuration = new_configuration
        deployment = None
        if self.id in new_configuration.active_deployment.nodes:
            deployment = new_configuration.active_deployment
        if new_configuration.target_deployment and self.id in new_configuration.target_deployment.nodes:
            deployment = new_configuration.target_deployment
        self.node = deployment.nodes.get(self.id, None) if deployment else None
        self.__read_repair_enabled = deployment.read_repair_enabled if deployment else False
        self.__background_repair_enabled = deployment.background_repair_enabled if deployment else False
        self.__background_repair_interval_seconds = deployment.background_repair_interval_seconds if deployment else 0
        self.__initialize_node_client_pool()
        # TODO: restart servers if addresses changed

    def __quote(self, key):
        return urllib.quote_plus(key, safe='/&')

    def __unquote(self, path):
        return urllib.unquote_plus(path)

    def __fetch_value(self, key, node_id):
        if __debug__: logging.debug('key: %s, node_id: %s', key, node_id)
        return self.__clients_for_nodes((node_id,))[0].get(key) or (None, None)

    def __fetch_timestamps(self, key, node_ids):
        if __debug__: logging.debug('key: %s', key)
        node_ids = dict(node_ids)
        node_ids.pop(self.node.id, None)
        if not node_ids:
            return []
        clients = self.__clients_for_nodes(node_ids)
        return self.__internal_cluster_client.stat(clients, key)

    def __clients_for_nodes(self, node_ids):
        return [self.__node_clients[node_id] for node_id in node_ids]

    def __propagate(self, key, timestamp, value, target_nodes):
        if __debug__: logging.debug('key: %s, target_nodes: %s', key, target_nodes)
        collector = self.__internal_cluster_client.set_collector(self.__clients_for_nodes(target_nodes), 1)
        self.__internal_cluster_client.set_async(collector, key, timestamp, value)

    def __read_repair(self, key, timestamp, value, node_ids):
        if __debug__: logging.debug('key: %s, timestamp: %s', key, timestamp)
        remote_timestamps = self.__fetch_timestamps(key, node_ids)
        if __debug__: logging.debug('remote: %s', [(client.tag, repr(remote_timestamp)) for client, remote_timestamp in remote_timestamps])
        newer = [(client, remote_timestamp) for client, remote_timestamp in remote_timestamps
                 if remote_timestamp and remote_timestamp > timestamp]

        if __debug__: logging.debug('newer: %s', [(client.tag, repr(remote_timestamp)) for client, remote_timestamp in newer])
        if newer:
            latest_client, latest_timestamp = newer[-1]
            latest_timestamp, latest_value = self.__fetch_value(key, latest_client.tag)
            if __debug__: logging.debug('latest_timestamp: %s', latest_timestamp)
            if latest_timestamp and latest_value:
                value = latest_value
                timestamp = latest_timestamp

        older = [(client, remote_timestamp) for client, remote_timestamp in remote_timestamps
                 if remote_timestamp != None and remote_timestamp < timestamp]
        if __debug__: logging.debug('older: %s', [(client.tag, repr(remote_timestamp)) for client, remote_timestamp in older])
        if older:
            older_node_ids = [client.tag for (client, remote_timestamp) in older]
            self.__propagate(key, timestamp, value, older_node_ids)

        return timestamp, value

    def __internal_get(self, callback, key):
        if __debug__: logging.debug('key: %s', key)
        timestamp, value = self.__store.get(key)
        callback(timestamp or 0, value)

    def __internal_set(self, callback, key, timestamp, value):
        if __debug__: logging.debug('key: %s', key)
        self.__store.set(key, timestamp, value)
        callback(timestamp)

    def __internal_stat(self, callback, key):
        if __debug__: logging.debug('key: %s', key)
        timestamp, value = self.__store.get(key)
        if __debug__: logging.debug('timestamp: %s', timestamp)
        callback(timestamp or 0)

    def __public_get(self, callback, key):
        if __debug__: logging.debug("key: %s", key)
        timestamp, value = None, None
        node_ids = self.__configuration.find_nodes_for_key(key)
        if self.node.id in node_ids:
            timestamp, value = self.__store.get(key)
        if self.__read_repair_enabled:
            new_timestamp, new_value = self.__read_repair(key, timestamp, value, node_ids)
            if new_timestamp > timestamp and self.node.id in node_ids:
                timestamp, value = new_timestamp, new_value
                self.__store.set(key, timestamp, value)
        callback(timestamp or 0, value)


    def __public_set(self, callback, key, timestamp, value):
        if __debug__: logging.debug("key: %s", key)
        node_ids = self.__configuration.find_nodes_for_key(key)
        if self.__read_repair_enabled:
            timestamp, value = self.__read_repair(key, timestamp, value, node_ids)
        if self.node.id in node_ids:
            self.__store.set(key, timestamp, value)
        callback(timestamp)

    def __public_stat(self, callback, key):
        def __get_callback(timestamp, value):
            callback(timestamp)
        self.__public_get(__get_callback, key)

    def __repair_key(self, key):
        node_ids = self.__configuration.find_nodes_for_key(key)
        if self.node.id not in node_ids:
            self.__store.remove(key)
        else:
            timestamp, value = self.__store.get(key)
            if timestamp:
                new_timestamp, new_value = self.__read_repair(key, timestamp, value, node_ids)
                if new_timestamp > timestamp:
                    self.__store.set(key, new_timestamp, new_value)

    def __repair_store(self):
        if not self.node:
            return
        if not self.__background_repair_enabled:
            return

        total_count = self.__store.count()
        scan_count = 0
        start_time = time.time()
        last_time = start_time
        elapsed_time = 0

        logging.info('Starting store repair. Total %d keys.', total_count)

        # TODO: Refactor away this cursor muck
        cursor = self.__store.cursor()
        while True:
            key = None
            try:
                cursor.next()
                key = cursor.key()
            except StopIteration:
                break
            except KeyError, e:
                logging.warning('KeyError: %s', e)
            except Exception, e:
                logging.warning('TC Exception: %s', e)
                # Not supposed to get this... pytc is broken.
                pass
            if not key:
                continue
            self.__repair_key(key)
            scan_count += 1
            now = time.time()
            if now - last_time > 5.0:
                last_time = now
                elapsed_time = now - start_time
                if __debug__: logging.debug('Store repair in progress. Scanned %d keys. Elapsed time: %s', scan_count, timedelta(seconds=elapsed_time))
            total_count = self.__store.count()
            if total_count == 0:
                break
            if not self.node:
                break
            if not self.__background_repair_enabled:
                break
            coio.sleep(self.__background_repair_interval_seconds / total_count)

        logging.info('Finished store repair. Scanned %d keys. Elapsed time: %s', scan_count, timedelta(seconds=elapsed_time))

    def __repair_task_loop(self):
        while True:
            start_time = time.time()
            try:
                self.__repair_store()
            except Exception, e:
                logging.exception(e)
                pass
            elapsed = time.time() - start_time
            spare_seconds = self.__background_repair_interval_seconds - elapsed
            if spare_seconds > 0:
                if __debug__: logging.debug('Healing task sleeping %s', timedelta(seconds=spare_seconds))
                coio.sleep(spare_seconds)

    def serve(self):
        self.__configuration_controller.start()
        while not self.__configuration:
            if __debug__: logging.debug('Waiting for configuration.')
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

        self.__repair_task = coio.stackless.tasklet(self.__repair_task_loop)()

        logging.info('Internal API: %s:%s' % (self.node.address, self.node.port))
        self.__server = service.Server(listener=('', self.node.port),
                                                services=(internal_service, public_service))
        self.__server.serve()

    def stop(self):
        self.__server.stop()
        self.__server = None
