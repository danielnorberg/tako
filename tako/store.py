# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import tc
import unittest
import logging
import struct

from syncless import coio

import utils
from utils import debug
from utils import testcase
from utils import timestamper

BUFFER_THRESHOLD = 4096

class Store(object):
    def __init__(self, filepath, commit_interval=0.5):
        self.operation_counter = 0
        super(Store, self).__init__()
        debug.log('filepath: %s', filepath)
        self.filepath = filepath
        self.db = tc.BDB()
        self.flusher = None
        self.commit_interval = commit_interval

    def open(self):
        self.db.open(self.filepath, tc.BDBOWRITER | tc.BDBOCREAT)
        self.begin()
        self.flusher = coio.stackless.tasklet(self._flush)()

    def close(self):
        self.flusher.kill()
        self.commit()
        self.db.close()

    def _flush(self):
        while True:
            debug.log('Committing %d operations', self.operation_counter)
            self.commit()
            self.operation_counter = 0
            self.begin()
            coio.sleep(self.commit_interval)

    def set(self, key, value, timestamp=None):
        self.operation_counter += 1
        timestamp = timestamp or timestamper.now()
        self.db.put(key, timestamper.pack(timestamp))
        self.db.putcat(key, value)
        return timestamp

    def set_timestamped(self, key, timestamped_value):
        self.operation_counter += 1
        self.db.put(key, timestamped_value)

    def unpack_timestamped_data(self, data):
        if len(data) > BUFFER_THRESHOLD:
            value = buffer(data, 8)
        else:
            value = data[8:]
        timestamp = self.read_timestamp(data)
        return value, timestamp

    def pack_timestamped_data(self, data, timestamp):
        return timestamper.pack(timestamp) + data

    def read_timestamp(self, data):
        return timestamper.unpack(data)

    def get_timestamped(self, key):
        try:
            return self.db.get(key)
        except:
            pass
        return None

    def get_timestamp(self, key):
        return self.get(key)[1]

    def get(self, key):
        value = None
        timestamp = None
        try:
            data = self.db.get(key)
            value, timestamp = self.unpack_timestamped_data(data)
            return (value, timestamp)
        except:
            pass
        return (None, None)

    def _jump(self, cur, start):
        keylen = len(start)
        cur.jump(start)
        key = cur.key()
        if keylen:
            while not key[:keylen] > start:
                cur.next()
                key = cur.key()
        return key

    # def _range(self, cur, start, end):
    #   try:
    #       endlen = len(end)
    #       key = self._jump(cur, start)
    #       while key[:endlen] <= end:
    #           yield key
    #           cur.next()
    #           key = cur.key()
    #   except KeyError:
    #       pass
    #
    # def get_key_value_range(self, start_key, end_key):
    #   """docstring for get_key_value_range"""
    #   cur = self.db.curnew()
    #   for key in self._range(cur, start_key, end_key):
    #       yield (key, self.unpack_timestamped_data(cur.val()))
    #
    # def get_key_range(self, start_key, end_key):
    #   """docstring for get_key_range"""
    #   cur = self.db.curnew()
    #   for key in self._range(cur, start_key, end_key):
    #       yield key

    def begin(self):
        self.db.tranbegin()

    def commit(self):
        self.db.trancommit()

class StoreTest(testcase.TestCase):
    def testStore(self):
        store = Store(filepath = self.tempfile())
        store.open()
        timestamp = store.set("foo", "bar")
        self.assertEqual(store.get("foo"), ("bar", timestamp))
        self.assertEqual(store.get("loo"), (None, None))
        store.close()
        store.open()
        self.assertEqual(store.get("foo"), ("bar", timestamp))
        store.close()

    # def testRange(self):
    #   store = Store(filepath = self.tempfile())
    #   store.open()
    #   ks = ['a', 'aa', 'ab', 'ba', 'bb', 'c']
    #   for k in ks:
    #       store.set(k, k)
    #   self.assertEqual(set(store.get_key_range('a', 'b')), set(['ba', 'bb']))
    #   self.assertEqual(set(store.get_key_range('', 'c')), set(ks))

    def testTransaction(self):
        """docstring for testTransaction"""
        store = Store(filepath = self.tempfile())
        store.open()
        store.begin()
        store.set('foo', 'foo')
        store.close()
        store.open()
        self.assertEqual(store.get('foo'), (None, None))
        store.begin()
        timestamp = timestamper.now()
        store.set('bar', 'bar', timestamp)
        self.assertEqual(store.get('bar'), ('bar', timestamp))
        store.commit()
        self.assertEqual(store.get('bar'), ('bar', timestamp))
        store.close()
        store.open()
        self.assertEqual(store.get('bar'), ('bar', timestamp))

    def testBuffer(self):
        key = 'foo'
        data = 'bar'
        store = Store(filepath = self.tempfile())
        store.open()
        timestamp = store.set(key, buffer(data))
        assert store.get(key) == (data, timestamp)

    def testPerf(self):
        import time
        store = Store(filepath = self.tempfile())
        data = 'bar' * 1024 * 1024 * 16
        M = len(data)
        store.open()
        start_time = time.time()
        N = 100
        for i in xrange(N):
            store.set(str(i), data)
        end_time = time.time()
        # store.close()
        elapsed_time = end_time - start_time
        print N / elapsed_time
        print '%.2f MB/s' % ((M * N / 1024.0 / 1024.0) / elapsed_time)


if __name__ == '__main__':
    unittest.main()
