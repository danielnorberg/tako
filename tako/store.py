import tc
import unittest
import logging
from utils import testcase
import struct

from utils.timestamp import Timestamp

class Store(object):
	"""docstring for Store"""
	def __init__(self, filepath):
		super(Store, self).__init__()
		logging.debug('filepath: %s', filepath)
		self.filepath = filepath
		self.db = tc.BDB()

	def open(self):
		"""docstring for open"""
		self.db.open(self.filepath, tc.BDBOWRITER | tc.BDBOCREAT)

	def close(self):
		"""docstring for close"""
		self.db.close()

	def set(self, key, value, timestamp=None):
		"""docstring for set"""
		timestamp = timestamp or Timestamp.now()
		logging.debug('key: %s, value: %s, timestamp: %s', repr(key), repr(value[0:16]), timestamp)
		data = struct.pack('Q', timestamp.microseconds) + value
		self.db.put(key, data)
		return timestamp

	def _read(self, data):
		value = data[8:]
		timestamp = Timestamp(struct.unpack('Q', data[0:8])[0])
		return value, timestamp

	def get(self, key):
		"""docstring for get"""
		logging.debug('%s', repr(key))
		value = None
		timestamp = None
		try:
			data = self.db.get(key)
			value, timestamp = self._read(data)
			logging.debug('key: %s, value: %s, timestamp: %s', repr(key), repr(value[0:16]), timestamp)
			return (value, timestamp)
		except:
			pass
		logging.debug('key: %s, value: None, timestamp: None', repr(key))
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

	def _range(self, cur, start, end):
		try:
			endlen = len(end)
			key = self._jump(cur, start)
			while key[:endlen] <= end:
				yield key
				cur.next()
				key = cur.key()
		except KeyError:
			pass

	def get_key_value_range(self, start_key, end_key):
		"""docstring for get_key_value_range"""
		cur = self.db.curnew()
		for key in self._range(cur, start_key, end_key):
			yield (key, self._read(cur.val()))

	def get_key_range(self, start_key, end_key):
		"""docstring for get_key_range"""
		cur = self.db.curnew()
		for key in self._range(cur, start_key, end_key):
			yield key

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

	def testRange(self):
		store = Store(filepath = self.tempfile())
		store.open()
		ks = ['a', 'aa', 'ab', 'ba', 'bb', 'c']
		for k in ks:
			store.set(k, k)
		self.assertEqual(set(store.get_key_range('a', 'b')), set(['ba', 'bb']))
		self.assertEqual(set(store.get_key_range('', 'c')), set(ks))

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
		timestamp = Timestamp.now()
		store.set('bar', 'bar', timestamp)
		self.assertEqual(store.get('bar'), ('bar', timestamp))
		store.commit()
		self.assertEqual(store.get('bar'), ('bar', timestamp))
		store.close()
		store.open()
		self.assertEqual(store.get('bar'), ('bar', timestamp))

if __name__ == '__main__':
	unittest.main()