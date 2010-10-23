import os
import kyotocabinet as kc

class KeyValueStore(object):
	"""docstring for KeyValueStore"""
	def __init__(self, db_file_name="key_value_db"):
		super(KeyValueStore, self).__init__()
		self.db_file_path = db_file_name + '.kch'
		self.db = kc.DB()

	def open(self):
		"""docstring for open"""
		if not self.db.open(self.db_file_path, kc.DB.OREADER | kc.DB.OWRITER | kc.DB.OCREATE):
			raise Exception('KeyValueStore open error', self.db.error())

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
		self.db.set(key, value)

	def get(self, key):
		"""docstring for get"""
		return self.db.get(key)

def testStore():
	store = KeyValueStore()
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