# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import unittest

from syncless import coio
from syncless import patch
patch.patch_socket()

import paths
paths.setup()

from tako import configuration
from tako.coordinatorclient import CoordinatorClient
from tako.coordinatorserver import CoordinatorServer
from tako.utils import testcase

class TestCoordinatorClient(testcase.TestCase):
    def callback(self, new_configuration):
        # logging.debug('timestamp: %s, new_configuration: %s', new_configuration.timestamp, new_configuration)
        self.new_configuration=new_configuration

    def testClient(self):
        cfg_filepath = 'test/local_cluster.yaml'
        cfg = configuration.try_load_file(paths.path(cfg_filepath))
        coordinator_server = CoordinatorServer(cfg.master_coordinator_id, cfg, paths.path(cfg_filepath))
        coordinator_server_task = coio.stackless.tasklet(coordinator_server.serve)()
        coio.stackless.schedule()
        self.new_configuration = None
        self.new_timestamp = None
        client = CoordinatorClient(coordinators=[cfg.master_coordinator], callbacks=[self.callback])
        client.start()
        for i in xrange(0, 1000):
            coio.sleep(0.01)
            if self.new_configuration or self.new_timestamp:
                break
        assert cfg.representation() == self.new_configuration.representation()
        print 'Fetched configuration: ', self.new_configuration
        print 'Timestamp: ', self.new_timestamp
        coordinator_server_task.kill()


if __name__ == '__main__':
    unittest.main()
