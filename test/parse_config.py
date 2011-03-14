import sys
import yaml
import paths
paths.setup()
import pprint
pp = pprint.PrettyPrinter(indent=4)
cfg = yaml.load(open(paths.path(sys.argv[1])))
pp.pprint(cfg)
