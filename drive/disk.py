import copy
import os
import json


# local disk
# A disk is consist fo a certain number of chunks
# A disk is consist of  a json file meta.json and several content files.
class SimpleDisk(object):
    def __init__(self, disk_id: int, disk_meta=None):
        if disk_meta is not None:
            self.meta = copy.deepcopy(disk_meta)

            self.chunk_size = disk_meta["chunk_size"]
            self.file_size = disk_meta["file_size"]

            self.chunk_per_disk = int(disk_meta["disk_size"] / disk_meta["chunk_size"])
            self.file_count = int(disk_meta["disk_size"] / disk_meta["file_size"])
            self.chunk_per_file = int(disk_meta["file_size"] / disk_meta["chunk_size"])

            self.meta["chunk_per_disk"] = self.chunk_per_disk
            self.meta["file_count"] = self.file_count
            self.meta["chunk_per_file"] = self.chunk_per_file

            # must be initialized in allocate or activate
            self.path = ""
            self.file_path = ""
            self.chunk_status = {}

            self.disk_id = disk_id
            self.meta["disk_id"] = self.disk_id
        else:
            self.disk_id = disk_id

    def allocate(self):
        disk_path = self.meta["raid_path"] + "\\%d" % self.disk_id
        if os.path.exists(disk_path):
            raise Exception("Disk initialization failed since location %s has been occupied." % disk_path)
        else:
            os.mkdir(disk_path)

            # update meta
            self.path = disk_path
            self.meta["path"] = disk_path

            for i in range(0, self.chunk_per_disk):
                self.meta["chunk_%d" % i] = False
                self.chunk_status[i] = False

            file_path = disk_path + "\\content"
            self.file_path = file_path
            self.meta["file_path"] = file_path

            # create disk
            for i in range(0, self.file_count):
                with open(file_path + "_%d.txt" % i, "wb") as f:
                    init_data = bytes(self.file_size)
                    f.write(init_data)

            # create meta
            meta_path = disk_path + "\\meta.json"
            with open(meta_path, "w+") as f:
                f.write(json.dumps(self.meta))

            print("Disk %d is initialized at %s." % (self.disk_id, self.path))

    # read meta
    def activate(self):
        disk_path = self.meta["raid_path"] + "\\%d" % self.disk_id
        if not os.path.exists(disk_path):
            raise Exception("Disk activation failed since no disk is located at %s." % disk_path)
        else:
            with open("%s\\meta.json" % disk_path) as f:
                self.meta = json.loads(f.read())
                assert self.disk_id == self.meta["disk_id"]

                # update meta
                self.path = disk_path

                file_path = disk_path + "\\content"
                self.file_path = file_path

                for i in range(0, self.chunk_per_disk):
                    self.chunk_status[i] = self.meta["chunk_%d" % i]

                print("Disk %d is activated at %s." % (self.disk_id, self.path))

    def save(self):
        # update meta
        for i in range(0, self.chunk_per_disk):
            self.meta["chunk_%d" % i] = self.chunk_status[i]

        # write meta
        with open("%s\\meta.json" % self.path, "w") as f:
            f.write(json.dumps(self.meta))

    @property
    def available_chunks(self):
        ret = []
        for key in self.chunk_status.keys():
            if not self.chunk_status[key]:
                ret.append(key)
        return ret

    def write_chunk(self, chunk_idx, chunk):
        # locate chunk
        outer_idx = chunk_idx / self.chunk_per_file
        inner_idx = chunk_idx % self.chunk_per_file
        # write
        with open(self.file_path + "_%d.txt" % outer_idx, "rb+") as f:
            f.seek(inner_idx * self.chunk_size)
            f.write(chunk)
            self.chunk_status[chunk_idx] = True

    def read_chunk(self, chunk_idx):
        # locate chunk
        outer_idx = chunk_idx / self.chunk_per_file
        inner_idx = chunk_idx % self.chunk_per_file
        chunk = None
        # write
        with open(self.file_path + "_%d.txt" % outer_idx, "rb") as f:
            f.seek(inner_idx * self.chunk_size)

            chunk = f.read(self.chunk_size)
        return chunk

    # label a chunk as parity chunk, thus it cannot be used for store stripes
    def set_parity(self, chunk_idx):
        self.chunk_status[chunk_idx] = True

    def set_status(self, chunk_idx, status):
        self.chunk_status[chunk_idx] = status

    def reset(self):
        reset_data = str(bytes(self.file_size), encoding='utf-8')
        for file_idx in range(0, self.file_count):
            with open(self.file_path + "_%d.txt" % file_idx, "w") as f:
                f.write(reset_data)
        for key in self.chunk_status.keys():
            self.chunk_status[key] = False
        print("Disk %d is reset." % self.disk_id)