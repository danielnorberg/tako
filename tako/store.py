import tc
import unittest
import logging
from utils import testcase
import struct

from utils.timestamp import Timestamp

BUFFER_THRESHOLD = 4096

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
		timestamp_data = struct.pack('Q', timestamp.microseconds)
		self.db.put(key, timestamp_data)
		self.db.putcat(key, value)
		return timestamp

	def set_timestamped(self, key, timestamped_value):
		"""docstring for set_timestamped"""
		logging.debug('key: %s, value: %s', repr(key), repr(timestamped_value[0:16]))
		self.db.put(key, timestamped_value)

	def parse_timestamped_data(self, data):
		if len(data) > BUFFER_THRESHOLD:
			value = buffer(data, 8)
		else:
			value = data[8:]
		timestamp = Timestamp(struct.unpack_from('!Q', data)[0])
		return value, timestamp

	def get_timestamped(self, key):
		"""docstring for get_timestamped"""
		logging.debug('%s', repr(key))
		value = None
		timestamp = None
		try:
			return self.db.get(key)
		except:
			pass
		logging.debug('key: %s, value: None, timestamp: None', repr(key))
		return None

	def get(self, key):
		"""docstring for get"""
		logging.debug('%s', repr(key))
		value = None
		timestamp = None
		try:
			data = self.db.get(key)
			value, timestamp = self.parse_timestamped_data(data)
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
			yield (key, self.parse_timestamped_data(cur.val()))

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