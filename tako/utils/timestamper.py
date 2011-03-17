import time
import struct

struct_object = struct.Struct('!Q')
pack = struct_object.pack
unpack = lambda s:struct_object.unpack(s)[0]

def dumps(microseconds):
    return '%012d' % microseconds

def loads(s):
    return long(s)

def try_loads(s):
    try:
        return loads(s)
    except ValueError:
        return None
    except TypeError:
        return None

def to_seconds(microseconds):
    return microseconds / 1000000.0

def from_seconds(seconds):
    return long(seconds * 1000000)

def now():
    return from_seconds(time.time())
