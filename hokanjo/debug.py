import logging.handlers
import logging
import os

def whoami():
    import sys
    return sys._getframe(1).f_code.co_name

def callersname():
    import sys
    return sys._getframe(2).f_code.co_name

def configure_logging(appname):
	formatter = logging.Formatter("%(asctime)-15s %(levelname)s (%(process)d) %(filename)s:%(lineno)d %(funcName)s(): %(message)s")
	handler = logging.StreamHandler()
	handler.setFormatter(formatter)
	logging.getLogger().addHandler(handler)
	logging.getLogger().setLevel(logging.DEBUG)