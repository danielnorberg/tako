# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from consistenthash import ConsistentHash
from utils.timedelta_parser import parse_timedelta

def timedelta_total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24.0 * 3600.0) * 10**6) / 10**6

def timedelta_days(td):
    return timedelta_total_seconds(td) / (24.0 * 3600.0)

class Node(object):
    def __init__(self, node_id, bucket_id, address, port):
        super(Node, self).__init__()
        self.id = node_id
        self.bucket_id = bucket_id
        self.address = address
        self.port = port

    def __str__(self):
        return "Node(id=%s, bucket=%s, %s:%d)" % (self.id, self.bucket_id, self.address, self.address, self.port)

    def __repr__(self):
        return str(self)

class Bucket(object):
    def __init__(self, bucket_id, nodes):
        super(Bucket, self).__init__()
        self.id = bucket_id
        self.nodes = nodes

    def __str__(self):
        return 'Bucket(%s)' % ', '.join([str(node) for node in self.nodes.itervalues()])

    def __repr__(self):
        return str(self)

    def __iter__(self):
        return self.nodes.itervalues()

    def representation(self):
        return dict((node.id, [node.address, node.port]) for node in self.nodes.itervalues())

    # Essential for consistent hashing, be careful when modifying!
    def __hash__(self):
      return hash(self.id)

class Deployment(object):
    def __init__(self, name, representation):
        super(Deployment, self).__init__()
        self.original_representation = representation
        self.name = name
        self.buckets = {}
        for bucket_id, bucket in representation['buckets'].iteritems():
            nodes = dict((node_id, Node(node_id, bucket_id, address, port)) for node_id, (address, port) in bucket.iteritems())
            self.buckets[bucket_id] = Bucket(bucket_id, nodes)
        self.nodes = dict((node_id, node) for bucket in self.buckets.itervalues() for node_id, node in bucket.nodes.iteritems())
        self.consistent_hash = ConsistentHash(self.buckets.values(), **representation.get('hash', {}))
        self.read_repair_enabled = representation.get('read_repair', True)
        self.background_repair_enabled = representation.get('background_repair', True)
        self.background_repair_interval = parse_timedelta(representation.get('background_repair_interval', '1d'))
        self.background_repair_interval_seconds = timedelta_total_seconds(self.background_repair_interval)
        if self.background_repair_enabled:
            if not self.background_repair_interval_seconds:
                raise Exception('Parsing error')

    def siblings(self, node_id):
        node = self.nodes[node_id]
        bucket = self.buckets[node.bucket_id]
        siblings = dict(bucket.nodes)
        del siblings[node_id]
        return siblings.values()

    def buckets_for_key(self, key):
        return self.consistent_hash.find_buckets(key)

    def representation(self):
        spec = {
                'read_repair': self.read_repair_enabled,
                'background_repair':self.background_repair_enabled,
                'background_repair_interval':str(self.background_repair_interval),
                'hash': {
                        'buckets_per_key': self.consistent_hash.buckets_per_key,
                },
                'buckets':dict((bucket_id, bucket.representation()) for bucket_id, bucket in self.buckets.iteritems()),
        }
        return spec

    def __str__(self):
        return 'Deployment(%s)' % self.spec()

    def __repr__(self):
        return str(self)

class Coordinator(object):
    def __init__(self, coordinator_id, address, port):
        super(Coordinator, self).__init__()
        self.id = coordinator_id
        self.address = address
        self.port = port

    def __str__(self):
        return "Coordinator(id=%s, %s:%d)" % (self.id, self.address, self.port)

    def __repr__(self):
        return str(self)

    def configuration_url(self):
        return 'http://%s:%d/configuration' % (self.address, self.port)
