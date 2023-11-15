from drive.disk import SimpleDisk
import os
class BaseController(object):
    def __init__(self, config):
        self.config = config
        pass

    def create_new_raid(self):
        pass


class SimpleController(BaseController):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.stripe_count = config.stripe_count
        self.parity_count = config.parity_count
        self.disks = []
        self.data_objs = {}

    def create_new_raid(self):
        assert self.config.disk_size % self.config.r == 0

        if not os.path.exists(self.config.name):
            os.mkdir(self.config.name)

        disk_meta = {
            "raid_path" : self.config.name,
            "file_size" : self.config.file_size,
            "chunk_size" : self.config.r,
            "disk_size" : self.config.disk_size,
            "chunk_per_disk" : self.config.disk_size / self.config.r,
        }
        for i in range(0, self.stripe_count + self.parity_count):
            disk = SimpleDisk(disk_id=i, disk_meta=disk_meta)
            disk.allocate()

    def activate_raid(self):
        assert self.config.disk_size % self.config.r == 0
        disk_meta = {
            "raid_path" : self.config.raid_path,
            "file_size" : self.config.file_size,
            "chunk_size" : self.config.r,
            "disk_size" : self.config.disk_size,
            "chunk_per_disk" : self.config.disk_size / self.config.r,
        }
        for i in range(0, self.stripe_count + self.parity_count):
            disk = SimpleDisk(disk_id=i, disk_meta=disk_meta)
            disk.allocate()