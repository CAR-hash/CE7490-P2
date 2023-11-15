import argparse
import json
from controller.controller import SimpleController

parser = argparse.ArgumentParser(prog="Distributed Raid-6", description="Raid commands")

parser.add_argument('-i', '--init', type=str, nargs=1)
parser.add_argument('-a', '--activate', type=str, nargs=1)

opts = parser.parse_args()

opt_count = 0


class ConfigObject(object):
    def __init__(self, config):
        config.replace("\n", "")
        dic = json.loads(config)
        for key in dic.keys():
            self.__setattr__(key, dic[key])


# create an RAID-6 instance according to the config file
if opts.init is not None:
    assert opt_count == 0
    opt_count = opt_count + 1
    config = opts.init[0]
    with open(config, 'r') as f:
        config = ConfigObject(f.read())
        controller = SimpleController(config)
        controller.create_new_raid()

if opts.activate is not None:
    assert opt_count == 0
    opt_count = opt_count + 1