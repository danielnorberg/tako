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
        self.assertEqual(store.count(), 1)
        self.assertEqual(store.get("foo"), (timestamp, "bar"))
        self.assertEqual(store.get("loo"), (None, None))
        store.close()
        store.open()
        self.assertEqual(store.get("foo"), (timestamp, "bar"))
        self.assertEqual(store.count(), 1)
        store.remove("foo")
        self.assertEqual(store.count(), 0)
        self.assertEqual(store.get("foo"), (None, None))
        store.close()

    def testTransaction(self):
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
        self.assertEqual(store.count(), 1)
        self.assertEqual(store.get('bar'), (timestamp, 'bar'))
        store.commit()
        self.assertEqual(store.get('bar'), (timestamp, 'bar'))
        self.assertEqual(store.count(), 1)
        store.close()
        store.open()
        self.assertEqual(store.count(), 1)
        self.assertEqual(store.get('bar'), (timestamp, 'bar'))

    def testBuffer(self):
        key = 'foo'
        data = 'bar'
        store = Store(filepath = self.tempfile())
        store.open()
        timestamp = timestamper.now()
        store.set(key, timestamp, buffer(data))
        self.assertEqual(store.count(), 1)
        assert store.get(key) == (timestamp, data)

    def testCursor(self):
        ks = ['aaa', 'aab', 'aba', 'abb', 'baa', 'bab', 'bba', 'bbb']
        vs = ks
        ts = [i for i, k in enumerate(ks)]
        f = self.tempfile()
        store = Store(filepath = f, auto_commit_interval=0)
        store.open()
        store.begin()
        for key, timestamp, value in zip(ks, ts, vs):
            store.set(key, timestamp, value)
        store.commit()
        cursor = store.cursor()
        self.assertEquals(list(cursor), ks)

    def testCursorModification(self):
        ks = ['aaa', 'aab', 'aba', 'abb', 'baa', 'bab', 'bba', 'bbb']
        vs = ks
        ts = [i for i, k in enumerate(ks)]
        f = self.tempfile()
        store = Store(filepath = f, auto_commit_interval=0)
        store.open()
        store.begin()
        for key, timestamp, value in zip(ks, ts, vs):
            store.set(key, timestamp, value)
        store.commit()
        cursor = store.cursor()
        store.remove('aaa')
        store.remove('aab')
        self.assertEquals(list(cursor), ks[2:])

if __name__ == '__main__':
    unittest.main()
