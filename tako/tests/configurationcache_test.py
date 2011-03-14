# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import logging
import os

import paths
paths.setup()

from tako import configuration
from tako.configurationcache import ConfigurationCache
from tako.utils import testcase
from tako.utils import timestamper

import paths
paths.setup()

class TestConfiguration(testcase.TestCase):
    def testPersistence(self):
        """docstring for testPersistence"""
        files = ['test/config.yaml', 'test/local_cluster.yaml', 'test/migration_1.yaml']
        for f in files:
            configuration_directory = self.tempdir()
            cache = ConfigurationCache(configuration_directory, 'test')
            filepath = paths.path(f)
            cfg = configuration.try_load_file(filepath)
            for i in xrange(0, 100):
                cfg.timestamp = timestamper.now()
                cache.cache_configuration(cfg)
                read_configuration = cache.get_configuration()
                self.assertEqual(read_configuration.representation(), cfg.representation())
                self.assertEqual(read_configuration.timestamp, cfg.timestamp)

if __name__ == '__main__':
    import unittest
    unittest.main()
