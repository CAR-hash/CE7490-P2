import argparse
import copy
import json
import os
from controller.controller import SimpleController, MutableController
from util.config_util import ConfigObject

parser = argparse.ArgumentParser(prog="Distributed Raid-6", description="Raid commands")

parser.add_argument('-i', '--init', type=str, nargs=1)
parser.add_argument('-a', '--activate', type=str, nargs=1)

opts = parser.parse_args()

opt_count = 0

# create an RAID-6 instance according to the config file
if opts.init is not None:
    assert opt_count == 0
    opt_count = opt_count + 1
    config = opts.init[0]
    with open(config, 'r') as f:
        config = ConfigObject(f.read())
        if config.mutable:
            controller = MutableController(config)
            controller.create_new_raid()
            controller.save()
        else:
            controller = SimpleController(config)
            controller.create_new_raid()
            controller.save()

if opts.activate is not None:
    assert opt_count == 0
    sys_name = opts.activate[0]

    if not os.path.exists(sys_name):
        raise Exception("No such system %s" % sys_name)

    with open("%s/meta.json" % sys_name, 'r') as f:
        config = ConfigObject(f.read())
        if config.mutable:
            controller = MutableController(config)
            controller.activate_raid()
        else:
            controller = SimpleController(config)
            controller.activate_raid()

        while True:
            cmd = input(">")
            cmd = cmd.split(" ")
            cmd[0] = "--" + cmd[0]
            parser = argparse.ArgumentParser(prog="Distributed Raid-6 Shell", description="Raid commands")
            parser.add_argument("--read", nargs=1)

            parser.add_argument("--write", nargs=1)
            parser.add_argument("-c", "--content", nargs=1, help="content to write to the object")
            parser.add_argument("-s", "--source", nargs=1, help="source file path to write to the object")

            parser.add_argument("--create", nargs=1)
            parser.add_argument("--list", action='store_true')

            parser.add_argument("-d", "--delete", nargs=1, help="delete an object")
            parser.add_argument("-a", '--all', action='store_true', help="delete all objects")

            parser.add_argument("--check", action='store_true', help="check if any disk is corrupted")

            parser.add_argument("--repair", nargs='+', help="repair the broken disks")

            parser.add_argument("--reset", action='store_true', help="reset the disks")

            parser.add_argument("--save", action='store_true')
            parser.add_argument("--quit", action='store_true')
            #try:
            opts = parser.parse_args(cmd)
            if opts.read is not None:
                print(type(controller))
                value = controller.read_obj(opts.read[0])
                print(value)
            elif opts.write is not None:
                if not any([opts.content is None, opts.source is None]):
                    raise Exception("Please specify the data to write.")
                elif opts.content is not None:
                    data = opts.content[0]
                    if len(data) > config.r and not config.mutable:
                        raise Exception("Minimal implementation does not support objects larger than 1 chunk.")
                    for i in range(len(data), config.r):
                        data = data + "0"
                    data = bytes(data, encoding="utf-8")
                    controller.write_obj(opts.write[0], data)
                elif opts.source is not None:
                    pass
                controller.save()
            elif opts.create is not None:
                controller.create_obj(opts.create[0])
                controller.save()
            elif opts.list:
                obj_list = controller.obj_list
                for obj in obj_list:
                    print(obj)
            elif opts.delete is not None:
                if opts.all:
                    to_remove = copy.deepcopy(controller.data_objs)
                    for key in to_remove:
                        controller.delete_obj(key)
                    controller.save()
                else:
                    controller.delete_obj(opts.delete[0])
                    controller.save()
            elif opts.check:
                print(controller.check_failure())
            elif opts.repair is not None:
                broken_disks = [int(i) for i in opts.repair]
                controller.repair(broken_disks)
            elif opts.reset:
                controller.reset()
            elif opts.save:
                controller.save()
            elif opts.quit:
                controller.save()
                break
            #except BaseException as e:
            #    print(e)
            #    print(e.__traceback__)
