# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from __future__ import with_statement

import yaml
import simplejson as json
import logging
import os

import paths
from utils import testcase
from utils.timestamp import Timestamp
from models import Coordinator, Deployment

def try_load_specification(specification, timestamp=Timestamp.now()):
    """docstring for try_load_specification"""
    try:
        return Configuration(specification, timestamp)
    except ValidationError, e:
        logging.error('Configuration is not valid: %s', e)
        return None

def try_load_json(s, timestamp=Timestamp.now()):
    try:
        specification = json.loads(s)
    except Exception, e:
        logging.error('Failed to parse JSON configuration: %s', e)
        return None
    return try_load_specification(specification, timestamp)

def try_load_file(filepath, timestamp=None):
    """docstring for load"""
    try:
        if not timestamp:
            timestamp = Timestamp.from_seconds(os.path.getmtime(filepath))
        with open(filepath) as f:
            specification = yaml.load(f)
    except OSError, e:
        logging.error('Failed reading configuration file: %s', e)
        return None
    except IOError, e:
        logging.error('Failed reading configuration file: %s', e)
        return None
    except Exception, e:
        logging.error('Failed reading configuration file: %s', e)
        return None
    return try_load_specification(specification, timestamp)

def try_dump_file(filepath, configuration):
    """docstring for try_dump_file"""
    logging.debug(filepath)
    try:
        dirpath = os.path.dirname(filepath)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        with open(filepath, 'w+') as f:
            yaml.dump(configuration.specification(), f)
        return True
    except IOError, e:
        logging.error('Failed to write configuration to file: %s', e)
        return False
    except OSError, e:
        logging.error('Failed to write configuration to file: %s', e)
        return False

def validate_specification(specification):
    """docstring for validate_specification"""
    # TODO: Complete validation
    # TODO: Validate that node id's are not recycled
    try:
        assert 'active_deployment' in specification
        assert 'deployments' in specification
        assert len(specification['deployments']) > 0
        assert specification['active_deployment'] in specification['deployments']
        if 'target_deployment' in specification:
            assert specification['target_deployment'] in specification['deployments']
        for deployment_id, deployment in specification['deployments'].iteritems():
            assert 'buckets' in deployment
            for bucket_id, bucket in deployment['buckets'].iteritems():
                assert len(bucket) > 0
                for node_id, node in bucket.iteritems():
                    assert len(node) == 3
                    address, http_port, raw_port = node
                    assert type(address) == str, 'type(address) == %s != str' % type(address)
                    assert type(http_port) == int, 'type(http_port) == %s != int' % type(http_port)
                    assert type(raw_port) == int, 'type(raw_port) == %s != int' % type(raw_port)
        if 'master_coordinator' in specification:
            assert specification['master_coordinator'] in specification['coordinators']
        for coordinator_id, coordinator in specification.get('coordinators', {}).iteritems():
            assert len(coordinator) == 2
            address, port = coordinator
            assert type(address) == str, 'type(address) == %s != str' % type(address)
            assert type(port) == int, 'type(port) == %s != int' % type(port)
    except AssertionError:
        raise
        # raise ValidationError()

class ValidationError(Exception):
    """docstring for ValidationError"""
    def __init__(self, description=None):
        super(ValidationError, self).__init__()
        self.description = description

    def __str__(self):
        """docstring for __str__"""
        return 'ValidationError(%s)' % (self.description or '')

    def __repr__(self):
        """docstring for __repr__"""
        return str(self)

class Configuration(object):
    """docstring for Configuration"""
    def __init__(self, specification=None, timestamp=Timestamp.now()):
        super(Configuration, self).__init__()
        self.timestamp = timestamp
        if specification:
            assert self.load(specification)

    def __repr__(self):
        """docstring for __repr__"""
        return str(self)

    def __str__(self):
        """docstring for fname"""
        return "Configuration(%s)" % self.specification()

    def load(self, specification):
        """docstring for load"""
        validate_specification(specification)
        self.original_specification = specification
        self.deployments = dict((name, Deployment(name, deployment_specification)) for name, deployment_specification in specification.get('deployments', {}).iteritems())
        self.active_deployment_name = specification['active_deployment']
        self.active_deployment = self.deployments[self.active_deployment_name]
        self.target_deployment_name = specification.get('target_deployment', None)
        self.target_deployment = self.deployments[self.target_deployment_name] if self.target_deployment_name else None
        self.coordinators = dict((coordinator_id, Coordinator(coordinator_id, address, port)) for coordinator_id, (address, port) in specification.get('coordinators', {}).iteritems())
        self.master_coordinator_id = specification.get('master_coordinator', None)
        self.master_coordinator = self.coordinators.get(self.master_coordinator_id, None)
        return True

    def specification(self):
        """docstring for yaml"""
        spec = {
                'active_deployment': self.active_deployment_name,
                'deployments': dict((deployment.name, deployment.specification()) for deployment in self.deployments.itervalues()),
        }
        if self.coordinators:
            spec['coordinators'] = dict((coordinator.id, [coordinator.address, coordinator.port]) for coordinator in self.coordinators.itervalues())
        if self.master_coordinator:
            spec['master_coordinator'] = self.master_coordinator_id
        if self.target_deployment_name:
            spec['target_deployment'] = self.target_deployment_name
        return spec

    def find_neighbour_nodes_for_key(self, key, local_node):
        """docstring for find_neighbour_nodes_for_key"""
        neighbour_nodes = dict([(node.id, node) for bucket in self.active_deployment.buckets_for_key(key) for node in bucket])
        if self.target_deployment:
            neighbour_nodes.update(dict([(node.id, node) for bucket in self.target_deployment.buckets_for_key(key) for node in bucket]))
        neighbour_nodes.pop(local_node.id, None)
        return neighbour_nodes

    def find_neighbour_nodes_for_node(self, local_node):
        node_bucket = self.active_deployment.buckets[local_node.bucket_id]
        neighbour_buckets = list(self.active_deployment.consistent_hash.find_neighbour_buckets(node_bucket))
        if self.target_deployment and local_node.id in self.target_deployment.nodes:
            neighbour_buckets.extend(self.target_deployment.consistent_hash.find_neighbour_buckets(node_bucket))
        neighbour_nodes = dict((node.id, node) for bucket in neighbour_buckets for node in bucket)
        neighbour_nodes.pop(local_node.id, None)
        return neighbour_nodes

class TestConfiguration(testcase.TestCase):
    def testParsing(self):
        files = ['test/config.yaml', 'test/local_cluster.yaml', 'test/migration.yaml']
        for f in files:
            print
            filepath = paths.path(f)
            with open(filepath) as specfile:
                loaded_specification = yaml.load(specfile)
                timestamp = Timestamp.from_seconds(os.path.getmtime(filepath))
                helper_loaded_configuration = try_load_file(filepath)
                manually_loaded_configuration = Configuration(loaded_specification, timestamp)
                self.assertEqual(manually_loaded_configuration.specification(), loaded_specification)
                self.assertEqual(manually_loaded_configuration.specification(), helper_loaded_configuration.specification())
                self.assertEqual(manually_loaded_configuration.timestamp, helper_loaded_configuration.timestamp)


if __name__ == '__main__':
    import unittest
    unittest.main()
