import yaml
import paths
import pprint
pp = pprint.PrettyPrinter(indent=4)
cfg = yaml.load(open(paths.path('test/config.yaml')))
pp.pprint(cfg)