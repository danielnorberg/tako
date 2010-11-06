import time

class Timestamp(object):
	"""docstring for Timestamp"""
	__slots__ = ['microseconds']
	@classmethod
	def now(cls):
		"""docstring for now"""
		return Timestamp.from_seconds(time.time())

	@classmethod
	def try_loads(self, s):
		"""docstring for try_loads"""
		try:
			return Timestamp.loads(s)
		except ValueError:
			return None
		except TypeError:
			return None

	@classmethod
	def loads(cls, s):
		"""docstring for loads"""
		return Timestamp(int(s))

	@classmethod
	def from_seconds(cls, seconds):
		"""docstring for from_seconds"""
		return Timestamp(int(seconds * 1000000))

	def __init__(self, microseconds):
		super(Timestamp, self).__init__()
		assert type(microseconds) == int
		self.microseconds = microseconds

	def to_seconds(self):
		"""docstring for to_seconds"""
		return float(self.microseconds) / 1000000

	def __str__(self):
		"""docstring for __str__"""
		return '%012d' % self.microseconds

	def __repr__(self):
		"""docstring for __repr__"""
		return str(self)

	def __eq__(self, timestamp):
		"""docstring for __eq__"""
		return timestamp and self.microseconds == timestamp.microseconds

	def __lt__(self, timestamp):
		"""docstring for __lt__"""
		return timestamp and self.microseconds < timestamp.microseconds

	def __le__(self):
		"""docstring for __le__"""
		return timestamp and self.microseconds <= timestamp.microseconds

	def __ne__(self, timestamp):
		"""docstring for __ne__"""
		return not timestamp or self.microseconds != timestamp.microseconds

	def __gt__(self, timestamp):
		"""docstring for __gt__"""
		return not timestamp or self.microseconds > timestamp.microseconds

	def __ge__(self, timestamp):
		"""docstring for __ge__"""
		return not timestamp or self.microseconds >= timestamp.microseconds

if __name__ == '__main__':
	for i in range(0,1000):
		timestamp = Timestamp.now()
		assert Timestamp.loads(str(timestamp)) == timestamp
		assert Timestamp.try_loads('foo') == None
