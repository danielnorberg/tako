import urllib
import logging
import os
import email.utils

import paths
paths.setup()

from utils import debug
from utils import timestamper
from utils import httpserver

from client import Client, ValueNotAvailableException

class ProxyServer(object):
    """docstring for ProxyServer"""
    def __init__(self, proxy_id, address, coordinator_addresses, explicit_configuration, var_directory='var'):
        super(ProxyServer, self).__init__()
        self.id = proxy_id
        self.address = address
        self.__http_handlers = (
                ('/values/', {'GET':self.__values_GET, 'POST':self.__values_POST}),
                ('/stat/', {'GET':self.__stat_GET}),
        )
        configuration_cache_directory = os.path.join(var_directory, 'etc')
        name = 'proxyserver-%s' % self.id
        self.__client = Client(name, coordinator_addresses, explicit_configuration, configuration_cache_directory)

    def __get_timestamp(self, env):
        try:
            return timestamper.try_loads(env.get('HTTP_X_TIMESTAMP', None)) or timestamper.now()
        except ValueError:
            raise httpserver.BadRequest()

    def __quote(self, key):
        return urllib.quote_plus(key, safe='/&')

    def __unquote(self, path):
        return urllib.unquote_plus(path)

    def __values_GET(self, start_response, path, body, env):
        debug.log('path: %s', path)
        key = self.__unquote(path)
        try:
            timestamp, value = self.__client.get_value(key)
        except ValueNotAvailableException:
            start_response('503 Service Unavailable', [])
            return ['']

        if timestamp and value:
            start_response('200 OK', [
                    ('Content-Type', 'application/octet-stream'),
                    ('Last-Modified', email.utils.formatdate(timestamper.to_seconds(timestamp))),
                    ('X-Timestamp', str(timestamp)),
            ])
            return [value]
        else:
            start_response('404 Not Found', [])
            return ['']

    def __values_POST(self, start_response, path, body, env):
        debug.log("path: %s", path)
        key = self.__unquote(path)
        value = body.read()
        timestamp = self.__get_timestamp(env)
        try:
            new_timestamp = self.__client.set_value(key, timestamp, value)
        except ValueNotAvailableException:
            start_response('503 Service Unavailable', [])
            return ['']

        if new_timestamp:
            start_response('200 OK', [
                    ('Content-Type', 'application/octet-stream'),
                    ('X-Timestamp', str(new_timestamp)),
            ])
            return ['']
        else:
            start_response('503 Service Unavailable', [])
            return ['']

    def __stat_GET(self, start_response, path, body, env):
        debug.log('path: %s', path)
        key = self.__unquote(path)

        try:
            timestamp = self.__client.stat_value(key)
        except ValueNotAvailableException:
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
        self.__client.connect()
        self.__http_server = httpserver.HttpServer(listener=self.address,
                                                 handlers=self.__http_handlers)
        self.__http_server.serve()
