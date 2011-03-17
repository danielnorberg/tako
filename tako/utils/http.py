# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import urllib3
from urlparse import urlparse
import logging

pool_cache = {}

def _connection_from_urlinfo(urlinfo):
    key = '%s://%s:%s/' % (urlinfo.scheme, urlinfo.hostname, urlinfo.port)
    pool = pool_cache.get(key, None)
    if not pool:
        pool = urllib3.connection_from_url(urlinfo.geturl())
        pool_cache[key] = pool
    return pool

def fetch(url, tries=3):
    urlinfo = urlparse(url)
    pool = _connection_from_urlinfo(urlinfo)
    if __debug__: logging.debug('url: %s, tries: %d', url, tries)
    for i in xrange(1, tries):
        try:
            if __debug__: logging.debug('urlinfo.path: %s', urlinfo.path)
            r = pool.get_url(urlinfo.path)
            return r.data, r.headers
        except IOError, e:
            logging.error('Error: %s', e)
            return (None, None)

def post(url, value, headers={}):
    urlinfo = urlparse(url)
    pool = _connection_from_urlinfo(urlinfo)
    pool.urlopen('POST', urlinfo.path, body=value, headers=headers)
