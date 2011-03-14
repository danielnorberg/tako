# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import unittest

import paths
paths.setup()

from tako.store import Store
from tako.utils import testcase
from tako.utils import timestamper

class StoreTest(testcase.TestCase):
    def testStore(self):
        store = Store(filepath = self.tempfile())
        store.open()
        timestamp = timestamper.now()
        store.set("foo", timestamp, "bar")
        self.assertEqual(store.get("foo"), (timestamp, "bar"))
        self.assertEqual(store.get("loo"), (None, None))
        store.close()
        store.open()
        self.assertEqual(store.get("foo"), (timestamp, "bar"))
        store.close()

    def testRange(self):
      store = Store(filepath = self.tempfile())
      store.open()
      ks = ['a', 'aa', 'ab', 'ba', 'bb', 'c']
      timestamp = timestamper.now()
      for k in ks:
          store.set(k, timestamp, k)
      self.assertEqual(set(store.get_key_range('a', 'b')), set(['ba', 'bb']))
      self.assertEqual(set(store.get_key_range('', 'c')), set(ks))

    def testTransaction(self):
        """docstring for testTransaction"""
        store = Store(filepath = self.tempfile(), auto_commit_interval=0)
        store.open()
        store.begin()
        timestamp = timestamper.now()
        store.set('foo', timestamp, 'foo')
        store.abort()
        store.close()
        store.open()
        self.assertEqual(store.get('foo'), (None, None))
        store.begin()
        store.set('bar', timestamp, 'bar')
        self.assertEqual(store.get('bar'), (timestamp, 'bar'))
        store.commit()
        self.assertEqual(store.get('bar'), (timestamp, 'bar'))
        store.close()
        store.open()
        self.assertEqual(store.get('bar'), (timestamp, 'bar'))

    def testBuffer(self):
        key = 'foo'
        data = 'bar'
        store = Store(filepath = self.tempfile())
        store.open()
        timestamp = timestamper.now()
        store.set(key, timestamp, buffer(data))
        assert store.get(key) == (timestamp, data)

if __name__ == '__main__':
    unittest.main()
