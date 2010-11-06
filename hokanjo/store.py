import tc
import os, unittest, tempfile
import logging
from utils import testcase
import struct

from utils.timestamp import Timestamp

KEY_PREFIX = 'k'
TIME_PREFIX = 't'

class Store(object):
	"""docstring for Store"""
	def __init__(self, filepath):
		super(Store, self).__init__()
		logging.debug('filepath: %s', filepath)
		self.filepath = filepath
		self.db = tc.HDB()

	def open(self):
		"""docstring for open"""
		self.db.open(self.filepath, tc.HDBOWRITER | tc.HDBOCREAT)

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

	def get(self, key):
		"""docstring for get"""
		logging.debug('%s', repr(key))
		value = None
		timestamp = None
		try:
			data = self.db.get(key)
			value = data[8:]
			timestamp = Timestamp(struct.unpack('Q', data[0:8])[0])
			logging.debug('key: %s, value: %s, timestamp: %s', repr(key), repr(value[0:16]), timestamp)
			return (value, timestamp)
		except:
			pass
		logging.debug('key: %s, value: None, timestamp: None', repr(key))
		return (None, None)

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

if __name__ == '__main__':
	unittest.main()