# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import argparse
from utils import debug
import os
import httpserver
import simplejson as json
import configuration
import logging

from configuration import Configuration
from utils.timestamp import Timestamp

class BadRequest(object):
    """docstring for BadRequest"""
    def __init__(self, description=''):
        super(BadRequest, self).__init__()
        self.description = description

    def __str__(self):
        """docstring for __str__"""
        return repr(self)

    def __repr__(self):
        """docstring for __repr__"""
        return "BadRequest('%s')" % self.description

class CoordinatorServer(object):
    def __init__(self, coordinator_id, configuration, configuration_filepath):
        super(CoordinatorServer, self).__init__()
        self.id = coordinator_id
        self.original_configuration = configuration
        self.configuration = Configuration(configuration.specification())
        self.configuration.timestamp = Timestamp.from_seconds(os.stat(configuration_filepath).st_mtime)
        logging.debug('timestamp: %s', self.configuration.timestamp)
        self.coordinator = configuration.coordinators[self.id]
        self.configuration_filepath = configuration_filepath
        self.http_handlers = (
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
        return [json.dumps(self.configuration.specification())]

    def serve(self):
        self.http_server = httpserver.HttpServer(listener=(self.coordinator.address, self.coordinator.port), handlers=self.http_handlers)
        self.http_server.serve()

def main():
    debug.configure_logging('coordinatorserver')

    parser = argparse.ArgumentParser(description="Tako Coordinator")
    parser.add_argument('-id', '--id', help='Server id. Default = 1', default='c1')
    parser.add_argument('-cfg','--config', help='Config file.', default='test/local_cluster.yaml')

    try:
        args = parser.parse_args()
    except IOError, e:
        logging.error(e)
        exit(-1)

    cfg = configuration.try_load_file(args.config)

    if not cfg:
        logging.error('Failed to load configuration.')
        exit(-1)

    logging.info('Tako Coordinator Starting')
    logging.info('Coordinator id: %s', args.id)
    logging.info('Config file: %s', args.config)
    logging.info('Serving up %s on port %d', args.config, cfg.coordinators[args.id].port)

    try:
        server = CoordinatorServer(args.id, cfg, args.config)
        server.serve()
    except KeyboardInterrupt:
        pass

    logging.info('Exiting...')

if __name__ == '__main__':
    import paths
    paths.setup()
    os.chdir(paths.home)
    main()
