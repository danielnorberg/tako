# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from __future__ import with_statement

import yaml
import os

import paths
paths.setup()

from tako.configuration import Configuration, try_load_file
from tako.utils import testcase
from tako.utils import timestamper

class TestConfiguration(testcase.TestCase):
    def testParsing(self):
        files = [
            'test/local_cluster.yaml',
            'test/migration_1.yaml',
            'test/migration_2.yaml',
            'test/migration_3.yaml',
            'examples/cluster.yaml',
            'examples/local_cluster.yaml',
            'examples/standalone.yaml'
        ]
        for f in files:
            print
            filepath = paths.path(f)
            with open(filepath) as specfile:

                loaded_representation = yaml.load(specfile)
                timestamp = timestamper.from_seconds(os.path.getmtime(filepath))

                helper_loaded_configuration = try_load_file(filepath)
                manually_loaded_configuration = Configuration(loaded_representation, timestamp)

                self.assertEqual(manually_loaded_configuration.representation(),
                                 helper_loaded_configuration.representation())

                self.assertEqual(manually_loaded_configuration.timestamp,
                                 helper_loaded_configuration.timestamp)

                self.assertEqual(Configuration(helper_loaded_configuration.representation(), helper_loaded_configuration.timestamp).representation(),
                                 helper_loaded_configuration.representation())

if __name__ == '__main__':
    import unittest
    unittest.main()
