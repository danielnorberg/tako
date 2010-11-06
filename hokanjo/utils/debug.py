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

def pformat():
	"""docstring for pformat"""
	pass

def configure_logging(appname, level=logging.INFO):
	formatter = logging.Formatter("%(asctime)-15s %(levelname)s (%(process)d) %(filename)s:%(lineno)d %(funcName)s(): %(message)s")
	handler = logging.StreamHandler()
	handler.setFormatter(formatter)
	logging.getLogger().addHandler(handler)
	logging.getLogger().setLevel(level)