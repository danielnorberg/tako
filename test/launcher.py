import yaml
import subprocess
import os
import time
import argparse

import paths

from tako.configuration import Configuration

def launch(configuration_filepath, profiling=False, debug=False):
	"""docstring for launch"""
	os.chdir(paths.home)
	profiling_cmd = lambda nodeid: profiling and '-p nodeserver-%s.prof' % nodeid or ''
	debug_cmd = '-d' if debug else ''
	absolute_configuration_filepath = paths.path(configuration_filepath)
	cfg = Configuration(yaml.load(open(absolute_configuration_filepath)))
	# coordinator_cmds = ['python bin/tako-coordinator -id %s -cfg test/local_cluster.yaml &> var/log/coordinator-%s.log' % (coordinator.id, coordinator.id) for coordinator in cfg.coordinators.itervalues()]
	# node_cmds = ['python bin/tako-node -id %s -c localhost 4701 %s %s &> var/log/node-%s.log' % (node.id, profiling_cmd(node.id), debug_cmd, node.id) for node in cfg.active_deployment.nodes.itervalues()]
	# for cmd in coordinator_cmds:
	# 	subprocess.Popen(cmd, shell=True)
	# time.sleep(1)
	node_cmds = ['python bin/tako-node -id %s -cfg %s %s %s &> var/log/node-%s.log' % (node.id, configuration_filepath, profiling_cmd(node.id), debug_cmd, node.id) for node in cfg.active_deployment.nodes.itervalues()]
	for cmd in node_cmds:
		subprocess.Popen(cmd, shell=True)

	try:
		while True:
			raw_input()
	except:
		pass

	print 'Exiting...'

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Launcher")
	parser.add_argument('-c', '--configuration', help='Configuration file.', default='test/local_cluster.yaml')
	parser.add_argument('-p', '--profiling', help='Enable profiling', action='store_true')
	parser.add_argument('-d', '--debug', help='Enable debug logging.', action='store_true')
	args = parser.parse_args()
	launch(args.configuration, args.profiling, args.debug)