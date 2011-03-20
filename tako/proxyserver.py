import urllib
import logging
import os
import email.utils

import paths
paths.setup()

from utils import timestamper
from utils import httpserver

from client import Client, NotAvailableException

class ProxyServer(object):
    def __init__(self, proxy_id, address, coordinator_addresses, explicit_configuration, var_directory='var', max_retries=30, retry_interval=1.0):
        super(ProxyServer, self).__init__()
        self.id = proxy_id
        self.address = address
        self.__http_handlers = (
                ('/values/', {'GET':self.__values_GET, 'POST':self.__values_POST}),
                ('/stat/', {'GET':self.__stat_GET}),
        )
        configuration_cache_directory = os.path.join(var_directory, 'etc')
        name = 'proxyserver-%s' % self.id
        self.__cluster_client = Client(
            name,
            coordinator_addresses=coordinator_addresses,
            explicit_configuration=explicit_configuration,
            configuration_cache_directory=configuration_cache_directory,
            max_retries=max_retries,
            retry_interval=retry_interval
        )

    def __get_timestamp(self, env):
        try:
            return timestamper.try_loads(env.get('HTTP_X_TIMESTAMP', None)) or timestamper.now()
        except ValueError:
            raise httpserver.BadRequest()

    def __values_GET(self, start_response, path, body, env):
        if __debug__: logging.debug('path: %s', path)
        key = urllib.unquote_plus(path)

        try:
            timestamp, value = self.__cluster_client.get(key)
        except NotAvailableException:
            start_response('503 Service Unavailable', [])
            return ['']

        if timestamp and value:
            start_response('200 OK', [
                    ('Content-Type', 'application/octet-stream'),
                    ('Last-Modified', email.utils.formatdate(timestamper.to_seconds(timestamp))),
                    ('X-Timestamp', timestamper.dumps(timestamp)),
            ])
            return [value]
        else:
            start_response('404 Not Found', [])
            return ['']

    def __values_POST(self, start_response, path, body, env):
        if __debug__: logging.debug("path: %s", path)
        key = urllib.unquote_plus(path)
        value = body.read()
        timestamp = self.__get_timestamp(env)

        try:
            new_timestamp = self.__cluster_client.set(key, timestamp, value)
        except NotAvailableException:
            start_response('503 Service Unavailable', [])
            return ['']

        if new_timestamp:
            start_response('200 OK', [
                    ('Content-Type', 'application/octet-stream'),
                    ('X-Timestamp', timestamper.dumps(new_timestamp)),
            ])
            return ['']
        else:
            start_response('503 Service Unavailable', [])
            return ['']

    def __stat_GET(self, start_response, path, body, env):
        if __debug__: logging.debug('path: %s', path)
        key = urllib.unquote_plus(path)

        try:
            timestamp = self.__cluster_client.stat(key)
        except NotAvailableException:
            start_response('503 Service Unavailable', [])
            return ['']

        if timestamp:
            start_response('200 OK', [
                    ('Content-Type', 'application/octet-stream'),
                    ('Last-Modified', email.utils.formatdate(timestamper.to_seconds(timestamp))),
                    ('X-Timestamp', timestamper.dumps(timestamp)),
            ])
            return [timestamper.dumps(timestamp)]
        else:
            start_response('404 Not Found', [])
            return ['']

    def serve(self):
        logging.info('Public HTTP API: %s:%s' % self.address)
        self.__cluster_client.connect()
        while True:
            try:
                self.__http_server = httpserver.HttpServer(listener=self.address,
                                                           handlers=self.__http_handlers)
                self.__http_server.serve()
            # Workaround for bug in syncless
            except NameError:
                pass

