import os
home = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data = os.path.join(home, 'data')
logs = os.path.join(home, 'logs')

def path(_path):
	"""docstring for path"""
	return os.path.join(home, _path)
