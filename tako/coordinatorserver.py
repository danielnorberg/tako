# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import logging
import os
import simplejson as json

from configuration import Configuration
from utils import debug
from utils import timestamper
from utils import httpserver

class CoordinatorServer(object):
    def __init__(self, coordinator_id, configuration, configuration_filepath):
        super(CoordinatorServer, self).__init__()
        self.id = coordinator_id
        self.original_configuration = configuration
        self.configuration = Configuration(configuration.representation())
        self.configuration.timestamp = timestamper.from_seconds(os.stat(configuration_filepath).st_mtime)
        logging.debug('timestamp: %s', self.configuration.timestamp)
        self.coordinator = configuration.coordinators[self.id]
        self.configuration_filepath = configuration_filepath
        self.__http_handlers = (
                ('/configuration', {'GET': self.configuration_GET}),
        )
        self.http_server = None

    def reload_configuration(self):
        pass

    def configuration_GET(self, start_response, path, body, env):
        logging.debug(str(self.configuration.timestamp))
        start_response('200 OK', [
                ('content-type', 'application/json'),
                ('x-timestamp', str(self.configuration.timestamp)),
        ])
        return [json.dumps(self.configuration.representation())]

    def serve(self):
        self.http_server = httpserver.HttpServer(listener=(self.coordinator.address, self.coordinator.port), handlers=self.__http_handlers)
        self.http_server.serve()
