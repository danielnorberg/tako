import yaml
import subprocess
import os, sys
import time

import paths
sys.path.insert(0, paths.home)
from hokanjo.configuration import Configuration

def launch():
	"""docstring for launch"""
	os.chdir(paths.home)
	cfg = Configuration(yaml.load(open(paths.path('test/local_cluster.yaml'))))
	coordinator_cmds = ['python bin/hokanjo-coordinator -id %s -cfg test/local_cluster.yaml &> var/log/coordinator-%s.log' % (coordinator.id, coordinator.id) for coordinator in cfg.coordinators.itervalues()]
	node_cmds = ['python bin/hokanjo-node -id %s -c localhost 4701 &> var/log/node-%s.log' % (node.id, node.id) for node in cfg.active_deployment.nodes.itervalues()]
	for cmd in coordinator_cmds:
		subprocess.Popen(cmd, shell=True)
	time.sleep(1)
	for cmd in node_cmds:
		subprocess.Popen(cmd, shell=True)

	try:
		while True:
			raw_input()
	except:
		pass

	print 'Exiting...'

if __name__ == '__main__':
	launch()