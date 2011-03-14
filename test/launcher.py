import argparse
import logging
import os
import signal
import subprocess
import time
import yaml

import paths
paths.setup()

from tako.configuration import Configuration
from tako.utils import debug

def launch(configuration_filepath, profiling=False, debug=False, proxies=[]):

    logging.info('Using configuration file: %s', configuration_filepath)

    os.chdir(paths.home)
    debug_arg = '-d' if debug else ''
    cfg = Configuration(yaml.load(open(configuration_filepath)))

    logging.info('Starting coordinator processes.')

    processes = []
    try:
        for coordinator in cfg.coordinators.itervalues():
            log_filepath = 'var/tako/log/coordinator-%s.log' % coordinator.id
            with open(log_filepath, 'wb') as logfile:
                logfile.truncate()
            cmd = 'python bin/tako-coordinator -id %(id)s -cfg %(cfg)s -l %(logfile)s %(debug)s' % {
                'id':coordinator.id,
                'cfg':configuration_filepath,
                'logfile':log_filepath,
                'debug':debug_arg,
            }
            proc = subprocess.Popen(cmd, shell=True)
            processes.append(proc)
            logging.info('Launched Coordinator "%s" (%s:%s) (pid: %d)',
                         coordinator.id, coordinator.address, coordinator.port, proc.pid)
            logging.debug('command: %s', cmd)

        time.sleep(1)

        logging.info('Done.')
        logging.info('Starting node processes.')

        node_profiling_arg = lambda nodeid: '-p nodeserver-%s.prof' % nodeid if profiling else ''
        cfg_arg = '' if cfg.coordinators else '-cfg %s' % configuration_filepath
        coordinator_arguments = ''.join(['-c %s %s' % (coordinator.address, coordinator.port) for
                                        coordinator in cfg.coordinators.itervalues()])
        nodes = dict(cfg.active_deployment.nodes)
        if cfg.target_deployment:
            nodes.update(cfg.target_deployment.nodes)
        for node_id, node in nodes.iteritems():
            log_filepath = 'var/tako/log/node-%s.log' % node_id
            with open(log_filepath, 'wb') as logfile:
                logfile.truncate()
            cmd = 'python bin/tako-node -id %(id)s %(coordinators)s %(profiling)s %(debug)s %(cfg)s -l %(logfile)s' % {
                'id':node_id,
                'coordinators':coordinator_arguments,
                'profiling':node_profiling_arg(node.id),
                'debug':debug_arg,
                'cfg':cfg_arg,
                'logfile':log_filepath
            }
            if args.skip and node_id in args.skip:
                logging.info('Skipped Node "%s". Command: %s', node_id, cmd)
            else:
                proc = subprocess.Popen(cmd, shell=True)
                processes.append(proc)
                logging.info('Launched Node "%s" (pid: %d)', node_id, proc.pid)
                logging.debug('command: %s', cmd)

        logging.info('Done.')
        logging.info('Starting proxy processes')
        for i, (address, port) in enumerate(proxies):
            proxy_id = 'p%d' % (i + 1)
            log_filepath = 'var/tako/log/proxy-%s.log' % proxy_id
            with open(log_filepath, 'wb') as logfile:
                logfile.truncate()
            cmd = 'python bin/tako-proxy -id %(id)s %(coordinators)s %(debug)s %(cfg)s -l %(logfile)s' % {
                'id':proxy_id,
                'coordinators':coordinator_arguments,
                'debug':debug_arg,
                'cfg':cfg_arg,
                'logfile':log_filepath
            }
            proc = subprocess.Popen(cmd, shell=True)
            processes.append(proc)
            logging.info('Launched Proxy "%s" (%s:%s) (pid: %d)', proxy_id, address, port, proc.pid)
            logging.debug('command: %s', cmd)
        logging.info('Done.')


        logging.info('Ctrl-C to exit.')

        try:
            while True:
                raw_input()
        except:
            print
            pass
    finally:
        logging.info('Terminating processes.')
        for process in processes:
            os.kill(process.pid, signal.SIGTERM)
        logging.info('Done.')

    logging.info('Exiting...')


if __name__ == '__main__':
    default_configuration_filepath = paths.path('test/current_configuration.yaml')

    parser = argparse.ArgumentParser(description="Launcher")
    parser.add_argument('-c', '--configuration', help='Configuration file.', default=default_configuration_filepath)
    parser.add_argument('-d', '--debug', help='Enable debug logging.', action='store_true')
    parser.add_argument('-s', '--skip', help='Skip node.', type=str, nargs='+')
    parser.add_argument('-p', '--proxy', help='Proxy Server (address port)', nargs=2, action='append')
    parser.add_argument('-prof', '--profiling', help='Enable profiling', action='store_true')
    args = parser.parse_args()

    logging_level = logging.DEBUG if args.debug else logging.INFO
    debug.configure_logging('Tako Node', logging_level)

    configuration_filepath = os.path.abspath(args.configuration)
    launch(configuration_filepath, args.profiling, args.debug, args.proxy or [])
