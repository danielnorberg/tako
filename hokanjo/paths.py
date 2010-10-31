import os
home = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data = os.path.join(home, 'var', 'data')
log = os.path.join(home, 'var', 'log')

def path(_path):
	"""docstring for path"""
	return os.path.join(home, _path)
