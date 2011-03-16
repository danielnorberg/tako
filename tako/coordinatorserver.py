# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import logging
import os
import simplejson as json

import configuration
from utils import debug
from utils import timestamper
from utils import httpserver

class CoordinatorServer(object):
    def __init__(self, coordinator_id, configuration_filepath):
        super(CoordinatorServer, self).__init__()
        self.id = coordinator_id
        self.configuration = None
        self.configuration_filepath = configuration_filepath
        self.__http_handlers = (
            ('/configuration', {'GET': self.configuration_GET}),
        )
        self.http_server = None
        self.reload_configuration()

    def reload_configuration(self):
        self.configuration = configuration.try_load_file(self.configuration_filepath)
        if not self.configuration:
            raise Exception('Failed to load configuration from file: "%s"', self.configuration_filepath)
        self.configuration.timestamp = timestamper.from_seconds(os.stat(self.configuration_filepath).st_mtime)
        self.coordinator = self.configuration.coordinators[self.id]
        logging.debug('timestamp: %s', self.configuration.timestamp)

    def configuration_GET(self, start_response, path, body, env):
        logging.info('%(REMOTE_ADDR)s [GET %(PATH_INFO)s %(SERVER_PROTOCOL)s]', env)
        start_response('200 OK', [
            ('content-type', 'application/json'),
            ('x-timestamp', str(self.configuration.timestamp)),
        ])
        return [json.dumps(self.configuration.representation())]

    def serve(self):
        logging.info('http://%s:%s/configuration', self.coordinator.address, self.coordinator.port)
        self.http_server = httpserver.HttpServer(listener=('', self.coordinator.port), handlers=self.__http_handlers)
        self.http_server.serve()
