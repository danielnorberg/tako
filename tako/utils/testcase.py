# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import unittest
import shutil
import processing
import tempfile, os
import debug
import logging

class TestCase(unittest.TestCase):

    def setUp(self):
        """docstring for setUp"""
        self.files = []
        self.directories = []
        self.processes = []
        if not hasattr(self, 'logging_is_configured'):
            debug.configure_logging(type(self).__name__, logging.INFO)
            self.logging_is_configured = True

    def tearDown(self):
        """docstring for tearDown"""
        for process in self.processes:
            process.terminate()
        for filepath in self.files:
            if os.path.isfile(filepath):
                os.unlink(filepath)
        for dirpath in self.directories:
            shutil.rmtree(dirpath)

    def tempdir(self):
        """docstring for tempdir"""
        dirpath = tempfile.mkdtemp()
        self.directories.append(dirpath)
        return dirpath

    def tempfile(self):
        """docstring for tempfile"""
        handle, filepath = tempfile.mkstemp()
        os.close(handle)
        self.files.append(filepath)
        return filepath

    def startProcess(self, target, **kwargs):
        process = processing.Process(target=target, **kwargs)
        self.processes.append(process)
        process.start()
