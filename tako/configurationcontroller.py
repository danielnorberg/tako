# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-
import logging

import paths
paths.setup()

from utils import debug

from configurationcache import ConfigurationCache
from coordinatorclient import CoordinatorClient
from models import Coordinator


class ConfigurationController(object):
    """docstring for ConfigurationController"""
    def __init__(self, name, coordinator_addresses, explicit_configuration, configuration_cache_directory,
                 update_configuration_callback, update_interval=5*60):
        super(ConfigurationController, self).__init__()
        self.name = name
        coordinators = [Coordinator(None, address, port) for address, port in coordinator_addresses]
        self.__coordinator_client = CoordinatorClient(coordinators=coordinators,
                                                      callbacks=[self.__evaluate_new_configuration],
                                                      interval=update_interval)
        self.__configuration_cache = None
        self.__explicit_configuration = explicit_configuration
        self.__update_configuration_callback = None

        if configuration_cache_directory:
            debug.log('Persistent configuration cache enabled.')
            self.__configuration_cache = ConfigurationCache(configuration_cache_directory, self.name)
        else:
            debug.log('No configuration cache directory., persistent configuration cache disabled.')

        self.configuration = None
        if explicit_configuration:
            debug.log('Using explicit configuration')
            self.__evaluate_new_configuration(explicit_configuration)
        elif self.__configuration_cache:
            cached_configuration = self.__configuration_cache.get_configuration()
            if cached_configuration:
                debug.log('Using cached configuration')
                self.__evaluate_new_configuration(cached_configuration)

        self.__update_configuration_callback = update_configuration_callback

        if not self.configuration and not coordinator_addresses:
            raise Exception('No configuration available.')

    def __evaluate_new_configuration(self, new_configuration):
        if not self.configuration or new_configuration.timestamp > self.configuration.timestamp:
            if self.configuration:
                logging.info('New configuration: %s', new_configuration)
            self.configuration = new_configuration
            if self.__configuration_cache:
                self.__configuration_cache.cache_configuration(self.configuration)
            if self.__update_configuration_callback:
                self.__update_configuration_callback(new_configuration)

    def start(self):
        if self.configuration and self.__update_configuration_callback:
            self.__update_configuration_callback(self.configuration)
        self.__coordinator_client.start()

    def stop(self):
        self.__coordinator_client.stop()
