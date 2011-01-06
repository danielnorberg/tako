import yaml
import subprocess
import os
import signal
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
    #       subprocess.Popen(cmd, shell=True)
    # time.sleep(1)

    print 'Starting processes.'
    if args.skip:
        print 'Printing launch commands for skipped nodes:'

    processes = []
    for node_id, node in cfg.active_deployment.nodes.iteritems():
        log_filepath = 'var/log/node-%s.log' % node_id
        with open(log_filepath, 'wb') as logfile:
            logfile.truncate()
        cmd = 'python bin/tako-node -id %s -cfg %s %s %s -l %s' % (node_id, configuration_filepath, profiling_cmd(node.id), debug_cmd, log_filepath)
        if node_id in args.skip:
            print cmd
        else:
            processes.append(subprocess.Popen(cmd, shell=True))

    print 'Launched. Ctrl-C to exit.'

    try:
        while True:
            raw_input()
    except:
        pass

    print 'Terminating processes.'

    for process in processes:
        os.kill(process.pid, signal.SIGKILL)

    print 'Done.'
    print 'Exiting...'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Launcher")
    parser.add_argument('-c', '--configuration', help='Configuration file.', default='test/local_cluster.yaml')
    parser.add_argument('-p', '--profiling', help='Enable profiling', action='store_true')
    parser.add_argument('-d', '--debug', help='Enable debug logging.', action='store_true')
    parser.add_argument('-s', '--skip', help='Skip node.', type=str, nargs='+')
    args = parser.parse_args()
    launch(args.configuration, args.profiling, args.debug)
