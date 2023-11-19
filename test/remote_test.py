import unittest
import random

from controller.controller import *
from util.config_util import ConfigObject
import numpy as np

class TestRemoteDisk(unittest.TestCase):
    def test_Q_repair(self):
        config = None
        with open("remote/meta.json", 'r') as f:
            config = ConfigObject(f.read())
            controller = MutableController(config)
            controller.activate_raid()
            controller.reset()

        char_set = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n',
                         'o','p','q','r','s','t','u','v','w','x','y','z',
                         '0','1','2','3','4','5','6','7','8','9']
        random_data = []
        obj_count = 48
        break_time = 100

        for i in range(0, obj_count):
            print("Creating object %d" % i)
            controller.create_obj("obj_%d" % i)
            data = bytes(''.join(np.random.default_rng().choice(char_set, config.r)), encoding='utf-8')
            controller.write_obj("obj_%d" % i, data)
            random_data.append(data)

        for i in range(0, break_time):
            output, origin = controller.known_q_tst()
            self.assertEqual(output, origin)

        controller.reset()
        controller.save()

    def test_repair(self):
        with open("remote/meta.json", 'r') as f:
            config = ConfigObject(f.read())
            controller = SimpleController(config)
            controller.activate_raid()
            controller.reset()

        char_set = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n',
                         'o','p','q','r','s','t','u','v','w','x','y','z',
                         '0','1','2','3','4','5','6','7','8','9']
        random_data = []
        obj_count = 48
        break_time = 100

        for i in range(0, obj_count):
            print("Creating object %d" % i)
            controller.create_obj("obj_%d" % i)
            data = bytes(''.join(np.random.default_rng().choice(char_set, config.r)), encoding='utf-8')
            controller.write_obj("obj_%d" % i, data)
            random_data.append(data)

        content_count = math.floor(config.disk_size / config.file_size)
        # randomly break two disks
        for i in range(0, break_time):
            break_disk = random.sample(list(range(0, 8)), 1)
            print(f"Test {i}: break disk {break_disk[0]}")
            # corrupt disks
            for disk_idx in break_disk:
                for file_idx in range(0, content_count):
                    with open("minimal/%d/content_%d.txt" % (disk_idx, file_idx), 'w') as f:
                        data = str(bytes(config.file_size), encoding='utf-8')
                        f.write(data)
            # repair
            self.assertTrue(controller.check_failure())
            controller.repair(break_disk)
            for o in range(0, obj_count):
                data_now = controller.read_obj("obj_%d" % o)
                #print(f"now:{data_now}, standard:{random_data[o]}")
                self.assertEqual(data_now, random_data[o])

        controller.save()


if __name__ == '__main__':
    unittest.main()