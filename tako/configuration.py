# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from __future__ import with_statement

import yaml
import simplejson as json
import logging
import os

import paths
paths.setup()

from utils import timestamper
from models import Coordinator, Deployment

import traceback

def try_load_representation(representation, timestamp=timestamper.now()):
    logging.debug('timestamp = %s, representation = %s', timestamp, representation)
    try:
        configuration = Configuration(representation, timestamp)
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
    if not 'active_deployment' in representation: raise ValidationError()
    if not 'deployments' in representation: raise ValidationError()
    if not len(representation['deployments']) > 0: raise ValidationError()
    if not representation['active_deployment'] in representation['deployments']: raise ValidationError()
    if 'target_deployment' in representation:
        if not representation['target_deployment'] in representation['deployments']: raise ValidationError()
    for deployment_id, deployment in representation['deployments'].iteritems():
        if not 'buckets' in deployment: raise ValidationError()
        for bucket_id, bucket in deployment['buckets'].iteritems():
            if not len(bucket) > 0: raise ValidationError()
            for node_id, node in bucket.iteritems():
                if not len(node) == 3: raise ValidationError()
                address, http_port, raw_port = node
                if not type(address) == str: raise ValidationError('type(address) == %s != str' % type(address))
                if not type(http_port) == int: raise ValidationError('type(http_port) == %s != int' % type(http_port))
                if not type(raw_port) == int: raise ValidationError('type(raw_port) == %s != int' % type(raw_port))
    if 'master_coordinator' in representation:
        if not representation['master_coordinator'] in representation['coordinators']: raise ValidationError()
    for coordinator_id, coordinator in representation.get('coordinators', {}).iteritems():
        if not len(coordinator) == 2: raise ValidationError()
        address, port = coordinator
        if not type(address) == str: raise ValidationError('type(address) == %s != str' % type(address))
        if not type(port) == int: raise ValidationError('type(port) == %s != int' % type(port))

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

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "Configuration(%s)" % self.representation()

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
