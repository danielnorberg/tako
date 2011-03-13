# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

from socketless.service import Protocol, Method

class InternalNodeServiceProtocol(Protocol):
    handshake = ('Tako Internal Node API Service', 'Tako Internal Node API Client')
    methods = dict(
        get  = Method('g', [('key', str)], [('timestamp', long), ('value', str)]), # key -> timestamp, value
        set  = Method('s', [('key', str), ('timestamp', long), ('value', str)], [('timestamp', long)]), # key, timestamp, value -> timestamp
        stat = Method('t', [('key', str)], [('timestamp', long)]), # key -> timestamp
    )

class PublicNodeServiceProtocol(Protocol):
    handshake = ('Tako Public Node API Service', 'Tako Public Node API Client')
    methods = dict(
        get  = Method('g', [('key', str)], [('timestamp', long), ('value', str)]), # key -> timestamp, value
        set  = Method('s', [('key', str), ('timestamp', long), ('value', str)], [('timestamp', long)]), # key, timestamp, value -> timestamp
        stat = Method('t', [('key', str)], [('timestamp', long)]), # key -> timestamp
    )

