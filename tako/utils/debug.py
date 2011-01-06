# -*- Mode: Python; tab-width: 4; indent-tabs-mode: nil; -*-

import logging.handlers
import logging
import os
import pprint

def whoami():
    import sys
    return sys._getframe(1).f_code.co_name

def callersname():
    import sys
    return sys._getframe(2).f_code.co_name

def pp():
    return pprint.PrettyPrinter(indent=4)

def nop(*args):
    pass

log = nop

def configure_logging(appname, level=logging.INFO, filename=None):
    global log
    if level <= logging.DEBUG:
        log = logging.debug
        format = "%(asctime)-15s %(levelname)s (%(process)d) %(filename)s:%(lineno)d %(funcName)s(): %(message)s"
    else:
        log = nop
        format="%(asctime)-15s %(levelname)s: %(message)s"
    logging.basicConfig(level=level, filename=filename, format=format)
