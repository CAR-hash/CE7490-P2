import copy
import os
import json


# local disk
# A disk is consist fo a certain number of chunks
# A disk is consist of  a json file meta.json and several content files.
class SimpleDisk(object):
    def __init__(self, disk_id: int, disk_meta):
        self.path = ""
        self.meta = copy.deepcopy(disk_meta)

        self.chunk_per_disk = int(self.meta["disk_size"]/self.meta["chunk_size"])
        self.file_count = int(self.meta["disk_size"]/self.meta["file_size"])
        self.chunk_per_file = int(self.meta["file_size"]/self.meta["chunk_size"])
        self.chunk_size = self.meta["chunk_size"]

        self.file_path = ""
        self.file_size = self.meta["file_size"]

        self.disk_id = disk_id

    def allocate(self):
        disk_path = self.meta["raid_path"] + "\\%d" % self.disk_id
        if os.path.exists(disk_path):
            raise Exception("Disk initialization failed since location %s has been occupied." % disk_path)
        else:
            os.mkdir(disk_path)

            #update meta
            self.meta["id"] = self.disk_id

            self.path = disk_path
            self.meta["path"] = disk_path

            for i in range(0, self.chunk_per_disk):
                self.meta["chunk_%d" % i] = False

            file_path = disk_path + "\\content"
            self.file_path = file_path

            # create meta
            meta_path = disk_path + "\\meta.json"
            with open(meta_path, "w+") as f:
                f.write(json.dumps(self.meta))
            # create disk
            for i in range(0, self.file_count):
                with open(file_path + "_%d" % i, "w+") as f:
                    init_data = bytes(self.file_size)
                    f.write(str(init_data, encoding="utf-8"))

            print("Disk %d is initialized at %s." % (self.disk_id, self.path))

    # read meta
    def activate(self):
        pass

    def write_chunk(self, chunk_idx, chunk):
        # locate chunk
        outer_idx = chunk_idx / self.chunk_per_file
        inner_idx = chunk_idx % self.chunk_per_file
        # write
        with open(self.file_path + "_%d" % outer_idx) as f:
            data = f.read()
            data[inner_idx*self.chunk_size:(inner_idx+1)*self.chunk_size] = chunk
        pass