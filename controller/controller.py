from drive.disk import SimpleDisk

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
        print(self.config)
        for i in range(0, self.stripe_count + self.parity_count):
            disk = SimpleDisk(disk_id=i)