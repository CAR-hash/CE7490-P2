import os
import json
RAID_PATH = "disk"

# local disk
# A disk is consist fo a certain number of chunks
# A disk is consist of  a json file meta.json and several content files.
class SimpleDisk(object):
    def __init__(self, disk_id: int):
        self.disk_id = disk_id
        self.path = ""
        self.allocate()

    def allocate(self):
        disk_path = RAID_PATH + "\\%d" % self.disk_id
        print(disk_path)
        if os.path.exists(disk_path):
            raise Exception("Disk initialization failed since location %s has been occupied." % disk_path)
        else:
            os.mkdir(disk_path)
            self.path = disk_path
            file_path = disk_path + "\\content"
            # create meta

            # create disk
            open(file_path, "w+")
            print("Disk %d is initialized at %s." % (self.disk_id, self.path))