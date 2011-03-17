# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import logging
import os

import paths
paths.setup()

import configuration
from utils import timestamper

MAX_CONFIGURATION_HISTORY = 10

class ConfigurationCache(object):
    def __init__(self, directory, name):
        super(ConfigurationCache, self).__init__()
        self.directory = os.path.abspath(directory)
        self.name = name
        logging.debug('name = "%s", directory = "%s"', self.name, self.directory)

    def filename_matches(self, filename):
        name, ext = os.path.splitext(filename)
        if ext != '.yaml':
            return False
        creator_name, timestamp_string = name.split('.')
        return creator_name == self.name

    def list_files(self):
        try:
            return sorted([filename for filename in os.listdir(self.directory) if self.filename_matches(filename)])
        except OSError, e:
            logging.debug(e)
            return []

    def cleanup(self):
        filenames = self.list_files()
        if len(filenames) > MAX_CONFIGURATION_HISTORY:
            remove_count = len(filenames) - MAX_CONFIGURATION_HISTORY
            remove_files = filenames[0:remove_count]
            logging.debug('Removing files: %s' % ', '.join([repr(filename) for filename in remove_files]))
            for filename in remove_files:
                filepath = os.path.join(self.directory, filename)
                try:
                    os.unlink(filepath)
                except OSError, e:
                    logging.error('Failed to remove configuration file: %s', e)

    def get_configuration(self):
        filenames = self.list_files()
        logging.debug('filenames = %s', filenames)
        filenames.reverse()
        for filename in filenames:
            name, ext = os.path.splitext(filename)
            creator_name, timestamp_string = name.split('.')
            timestamp = timestamper.loads(timestamp_string)
            filepath = os.path.join(self.directory, filename)
            persisted_configuration = configuration.try_load_file(filepath, timestamp)
            if persisted_configuration:
                return persisted_configuration
        return None

    def cache_configuration(self, cfg):
        filename = '%s.%s.yaml' % (self.name, cfg.timestamp)
        filepath = os.path.join(self.directory, filename)
        if configuration.try_dump_file(filepath, cfg):
            self.cleanup()
            return True
        return False
