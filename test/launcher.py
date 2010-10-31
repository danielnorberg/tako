import yaml
import subprocess
import os, sys

import paths
sys.path.insert(0, paths.home)
from hokanjo.configuration import Configuration

def launch():
	"""docstring for launch"""
	os.chdir(paths.home)
	cfg = Configuration(yaml.load(open(paths.path('test/config.yaml'))))
	cmds = ['python hokanjo/nodeserver.py -id %d -f var/data/%d.tch -cfg test/config.yaml &> var/log/%d.log' % (node.id, node.id, node.id) for node in cfg.active_deployment.nodes.itervalues()]
	processes = [subprocess.Popen(cmd, shell=True) for cmd in cmds]
	try:
		while True:
			raw_input()
	except:
		pass
	print 'Exiting...'

if __name__ == '__main__':
	launch()