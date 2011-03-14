# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import logging

from syncless import coio

import paths
paths.setup()

import configuration
from utils import timestamper
from utils import http

class CoordinatorClient(object):
    def __init__(self, coordinators=[], callbacks=None, interval=30):
        super(CoordinatorClient, self).__init__()
        self.coordinators = coordinators
        self.callbacks = callbacks
        self.interval = interval
        self.configuration_fetcher = None
        self.configuration = None

    def start(self):
        self.configuration_fetcher = coio.stackless.tasklet(self.__fetch_configurations)()

    def stop(self):
        if self.configuration_fetcher:
            self.configuration_fetcher.kill()
            self.configuration_fetcher = None

    def __notify(self):
        logging.debug('configuration: %s', self.configuration)
        for f in self.callbacks:
            f(self.configuration)

    def __fetch_configuration(self, coordinator):
        # logging.debug('coordinator: %s', coordinator)
        url = coordinator.configuration_url()
        body, info = http.fetch(url)
        if body:
            # logging.debug('Got representation: %s', body)
            new_timestamp = timestamper.try_loads(info.get('x-timestamp', None))
            if new_timestamp:
                new_configuration = configuration.try_load_json(body, timestamp=new_timestamp)
                return (new_configuration, coordinator)
        return (None, coordinator)

    def __set_configuration(self, new_configuration):
        self.configuration = new_configuration
        self.coordinators = new_configuration.coordinators.values()

    def __fetch_configurations(self):
        while True:
            logging.debug('coordinators: %s', self.coordinators)
            if self.coordinators:
                configurations = []
                for coordinator in self.coordinators:
                    configurations.append(self.__fetch_configuration(coordinator))
                configurations.sort()
                for new_configuration, source_coordinator in configurations:
                    if new_configuration and (not self.configuration or new_configuration.timestamp > self.configuration.timestamp):
                        self.__set_configuration(new_configuration)
                        self.__notify()
                        break
            coio.sleep(self.interval)

