import yaml
import subprocess
import os
import signal
import argparse
import logging

import paths
paths.setup()

from tako.configuration import Configuration
from tako.utils import debug

def launch(configuration_filepath, profiling=False, debug=False):

    logging.info('Using configuration file: %s', configuration_filepath)

    os.chdir(paths.home)
    profiling_cmd = lambda nodeid: profiling and '-p nodeserver-%s.prof' % nodeid or ''
    debug_cmd = '-d' if debug else ''
    absolute_configuration_filepath = paths.path(configuration_filepath)
    cfg = Configuration(yaml.load(open(absolute_configuration_filepath)))

    logging.info('Starting coordinator processes.')

    processes = []

    for coordinator in cfg.coordinators.itervalues():
        cmd = 'python bin/tako-coordinator -id %s -cfg %s %s &> var/log/coordinator-%s.log' % (coordinator.id, configuration_filepath, debug_cmd, coordinator.id)
        proc = subprocess.Popen(cmd, shell=True)
        processes.append(proc)
        logging.info('Launched Coordinator "%s" (pid: %d)', coordinator.id, proc.pid)
        logging.debug('command: %s', cmd)

    logging.info('Done.')
    logging.info('Starting node processes.')

    coordinator_arguments = ''.join(['-c %s %s' % (coordinator.address, coordinator.port) for coordinator in cfg.coordinators.itervalues()])
    for node_id, node in cfg.active_deployment.nodes.iteritems():
        log_filepath = 'var/log/node-%s.log' % node_id
        with open(log_filepath, 'wb') as logfile:
            logfile.truncate()
        # cmd = 'python bin/tako-node -id %s -cfg %s %s %s -l %s' % (node_id, configuration_filepath, profiling_cmd(node.id), debug_cmd, log_filepath)
        cmd = 'python bin/tako-node -id %s %s %s %s -l %s' % (node_id, coordinator_arguments, profiling_cmd(node.id), debug_cmd, log_filepath)
        if args.skip and node_id in args.skip:
            logging.info('Skipped Node "%s". Command: %s', node_id, cmd)
        else:
            proc = subprocess.Popen(cmd, shell=True)
            processes.append(proc)
            logging.info('Launched Node "%s" (pid: %d)', node_id, proc.pid)
            logging.debug('command: %s', cmd)

    logging.info('Done.')
    logging.info('Ctrl-C to exit.')

    try:
        while True:
            raw_input()
    except:
        pass

    logging.info('Terminating processes.')

    for process in processes:
        os.kill(process.pid, signal.SIGTERM)

    logging.info('Done.')
    logging.info('Exiting...')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Launcher")
    parser.add_argument('-c', '--configuration', help='Configuration file.', default='test/local_cluster.yaml')
    parser.add_argument('-p', '--profiling', help='Enable profiling', action='store_true')
    parser.add_argument('-d', '--debug', help='Enable debug logging.', action='store_true')
    parser.add_argument('-s', '--skip', help='Skip node.', type=str, nargs='+')
    args = parser.parse_args()

    logging_level = logging.DEBUG if args.debug else logging.INFO
    debug.configure_logging('Tako Node', logging_level)

    launch(args.configuration, args.profiling, args.debug)
