import os
import tc

class Store(object):
	"""docstring for Store"""
	def __init__(self, db_file_path="key_value_db.tch"):
		super(Store, self).__init__()
		self.db_file_path = db_file_path
		self.db = tc.HDB()

	def open(self):
		"""docstring for open"""
		self.db.open(self.db_file_path, tc.HDBOREADER | tc.HDBOWRITER | tc.HDBOCREAT)

	def delete(self):
		"""docstring for delete"""
		try:
			os.unlink(self.db_file_path)
		except OSError:
			pass

	def close(self):
		"""docstring for close"""
		self.db.close()

	def set(self, key, value):
		"""docstring for set"""
		self.db.put(key, value)

	def get(self, key):
		"""docstring for get"""
		return self.db.get(key)

def testStore():
	store = Store()
	store.delete()
	store.open()
	store.set("foo", "bar")
	assert(store.get("foo") == "bar")
	store.close()
	store.open()
	assert(store.get("foo") == "bar")
	store.close()
	store.delete()

def main():
	testStore()

if __name__ == '__main__':
	# import hotshot
	# prof = hotshot.Profile("hotshot.prof")
	# prof.runcall(main)
	# prof.close()
	main()