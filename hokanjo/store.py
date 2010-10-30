import tc
import os, unittest, tempfile
import logging
import testcase


class Store(object):
	"""docstring for Store"""
	def __init__(self, filepath="store.tch"):
		super(Store, self).__init__()
		self.filepath = filepath
		self.db = tc.HDB()

	def open(self):
		"""docstring for open"""
		self.db.open(self.filepath, tc.HDBOWRITER | tc.HDBOCREAT)

	def close(self):
		"""docstring for close"""
		self.db.close()

	def set(self, key, value):
		"""docstring for set"""
		logging.debug('%s:%s', repr(key), repr(value))
		self.db.put(key, value)

	def get(self, key):
		"""docstring for get"""
		logging.debug('%s', repr(key))
		try:
			value = self.db.get(key)
			logging.debug('%s:%s', repr(key), repr(value))
			return value
		except:
			logging.debug('%s:None', repr(key))
		return None

class StoreTest(testcase.TestCase):
	def testStore(self):
		store = Store(filepath = self.tempfile())
		store.open()
		store.set("foo", "bar")
		self.assertEqual(store.get("foo"), "bar")
		self.assertEqual(store.get("loo"), None)
		store.close()
		store.open()
		self.assertEqual(store.get("foo"), "bar")
		store.close()

if __name__ == '__main__':
	unittest.main()