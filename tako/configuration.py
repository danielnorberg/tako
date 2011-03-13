# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from __future__ import with_statement

import yaml
import simplejson as json
import logging
import os

import paths
paths.setup()

from utils import debug
from utils import testcase
from utils import timestamper
from models import Coordinator, Deployment

def try_load_representation(representation, timestamp=timestamper.now()):
    try:
        logging.debug(representation)
        configuration = Configuration(representation, timestamp)
        logging.debug(configuration)
        logging.debug(configuration.representation())
        return configuration
    except ValidationError, e:
        logging.error('Configuration is not valid: %s', e)
        return None

def try_load_json(s, timestamp=timestamper.now()):
    try:
        representation = json.loads(s)
    except Exception, e:
        logging.error('Failed to parse JSON configuration: %s', e)
        return None
    return try_load_representation(representation, timestamp)

def try_load_file(filepath, timestamp=None):
    try:
        if not timestamp:
            timestamp = timestamper.from_seconds(os.path.getmtime(filepath))
        with open(filepath) as f:
            representation = yaml.load(f)
    except OSError, e:
        logging.error('Failed reading configuration file: %s', e)
        return None
    except IOError, e:
        logging.error('Failed reading configuration file: %s', e)
        return None
    except Exception, e:
        logging.error('Failed reading configuration file: %s', e)
        return None
    return try_load_representation(representation, timestamp)

def try_dump_file(filepath, configuration):
    logging.debug(filepath)
    try:
        dirpath = os.path.dirname(filepath)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        with open(filepath, 'w+') as f:
            yaml.dump(configuration.representation(), f)
        return True
    except IOError, e:
        logging.error('Failed to write configuration to file: %s', e)
        return False
    except OSError, e:
        logging.error('Failed to write configuration to file: %s', e)
        return False

def validate_representation(representation):
    # TODO: Complete validation
    # TODO: Validate that node id's are not recycled/nodes are not changed
    try:
        assert 'active_deployment' in representation
        assert 'deployments' in representation
        assert len(representation['deployments']) > 0
        assert representation['active_deployment'] in representation['deployments']
        if 'target_deployment' in representation:
            assert representation['target_deployment'] in representation['deployments']
        for deployment_id, deployment in representation['deployments'].iteritems():
            assert 'buckets' in deployment
            for bucket_id, bucket in deployment['buckets'].iteritems():
                assert len(bucket) > 0
                for node_id, node in bucket.iteritems():
                    assert len(node) == 3
                    address, http_port, raw_port = node
                    assert type(address) == str, 'type(address) == %s != str' % type(address)
                    assert type(http_port) == int, 'type(http_port) == %s != int' % type(http_port)
                    assert type(raw_port) == int, 'type(raw_port) == %s != int' % type(raw_port)
        if 'master_coordinator' in representation:
            assert representation['master_coordinator'] in representation['coordinators']
        for coordinator_id, coordinator in representation.get('coordinators', {}).iteritems():
            assert len(coordinator) == 2
            address, port = coordinator
            assert type(address) == str, 'type(address) == %s != str' % type(address)
            assert type(port) == int, 'type(port) == %s != int' % type(port)
    except AssertionError:
        raise
        # raise ValidationError()

class ValidationError(Exception):
    def __init__(self, description=None):
        super(ValidationError, self).__init__()
        self.description = description

    def __str__(self):
        return 'ValidationError(%s)' % (self.description or '')

    def __repr__(self):
        return str(self)

class Configuration(object):
    def __init__(self, representation=None, timestamp=timestamper.now()):
        super(Configuration, self).__init__()
        self.timestamp = timestamp
        if representation:
            assert self.load(representation)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "Configuration(%s)" % self.representation()

    def load(self, representation):
        validate_representation(representation)
        self.original_representation = representation
        self.deployments = dict((name, Deployment(name, deployment_representation)) for name, deployment_representation in representation.get('deployments', {}).iteritems())
        self.active_deployment_name = representation['active_deployment']
        self.active_deployment = self.deployments[self.active_deployment_name]
        self.target_deployment_name = representation.get('target_deployment', None)
        self.target_deployment = self.deployments[self.target_deployment_name] if self.target_deployment_name else None
        self.coordinators = dict((coordinator_id, Coordinator(coordinator_id, address, port)) for coordinator_id, (address, port) in representation.get('coordinators', {}).iteritems())
        self.master_coordinator_id = representation.get('master_coordinator', None)
        self.master_coordinator = self.coordinators.get(self.master_coordinator_id, None)
        return True

    def representation(self):
        spec = {
                'active_deployment': self.active_deployment_name,
                'deployments': dict((deployment.name, deployment.representation()) for deployment in self.deployments.itervalues()),
        }
        if self.coordinators:
            spec['coordinators'] = dict((coordinator.id, [coordinator.address, coordinator.port]) for coordinator in self.coordinators.itervalues())
        if self.master_coordinator:
            spec['master_coordinator'] = self.master_coordinator_id
        if self.target_deployment_name:
            spec['target_deployment'] = self.target_deployment_name
        return spec

    def find_nodes_for_key(self, key):
        nodes = dict([(node.id, node) for bucket in self.active_deployment.buckets_for_key(key) for node in bucket])
        if self.target_deployment:
            nodes.update(dict([(node.id, node) for bucket in self.target_deployment.buckets_for_key(key) for node in bucket]))
        return nodes

    def find_neighbour_nodes_for_node(self, local_node):
        neighbour_buckets = []
        if local_node.id in self.active_deployment.nodes:
            deployment_node_bucket = self.active_deployment.buckets.get(local_node.bucket_id, None)
            neighbour_buckets.extend(self.active_deployment.consistent_hash.find_neighbour_buckets(deployment_node_bucket))
        if self.target_deployment and local_node.id in self.target_deployment.nodes:
            target_node_bucket = self.target_deployment.buckets.get(local_node.bucket_id, None)
            neighbour_buckets.extend(self.target_deployment.consistent_hash.find_neighbour_buckets(target_node_bucket))
        neighbour_nodes = dict((node.id, node) for bucket in neighbour_buckets for node in bucket)
        neighbour_nodes.pop(local_node.id, None)
        return neighbour_nodes

    def all_nodes(self):
        nodes = dict((node.id, node) for bucket in self.active_deployment.buckets.itervalues() for node in bucket)
        if self.target_deployment:
            nodes.update([(node.id, node) for bucket in self.target_deployment.buckets.itervalues() for node in bucket])
        return nodes

class TestConfiguration(testcase.TestCase):
    def testParsing(self):
        files = ['test/config.yaml', 'test/local_cluster.yaml', 'test/migration.yaml']
        for f in files:
            print
            filepath = paths.path(f)
            with open(filepath) as specfile:
                loaded_representation = yaml.load(specfile)
                timestamp = timestamper.from_seconds(os.path.getmtime(filepath))
                helper_loaded_configuration = try_load_file(filepath)
                manually_loaded_configuration = Configuration(loaded_representation, timestamp)
                self.assertEqual(manually_loaded_configuration.representation(), loaded_representation)
                self.assertEqual(manually_loaded_configuration.representation(), helper_loaded_configuration.representation())
                self.assertEqual(manually_loaded_configuration.timestamp, helper_loaded_configuration.timestamp)

if __name__ == '__main__':
    import unittest
    unittest.main()
