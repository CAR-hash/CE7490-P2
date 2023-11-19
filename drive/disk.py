import copy
import math
import os
import json

import requests

if os.path.exists("drive"):
    from drive.peer_gate import PeerGate
else:
    from peer_gate import PeerGate

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
        disk_path = self.meta["raid_path"] + "/%d" % self.disk_id
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

            file_path = disk_path + "/content"
            self.file_path = file_path
            self.meta["file_path"] = file_path

            # create disk
            for i in range(0, self.file_count):
                with open(file_path + "_%d.txt" % i, "wb") as f:
                    init_data = bytes(self.file_size)
                    f.write(init_data)

            # create meta
            meta_path = disk_path + "/meta.json"
            with open(meta_path, "w+") as f:
                f.write(json.dumps(self.meta))

            print("Disk %d is initialized at %s." % (self.disk_id, self.path))

    # read meta
    def activate(self):
        disk_path = self.meta["raid_path"] + "/%d" % self.disk_id
        if not os.path.exists(disk_path):
            raise Exception("Disk activation failed since no disk is located at %s." % disk_path)
        else:
            with open("%s/meta.json" % disk_path) as f:
                self.meta = json.loads(f.read())
                assert self.disk_id == self.meta["disk_id"]

                # update meta
                self.path = disk_path

                file_path = disk_path + "/content"
                self.file_path = file_path

                for i in range(0, self.chunk_per_disk):
                    self.chunk_status[i] = self.meta["chunk_%d" % i]

                print("Disk %d is activated at %s." % (self.disk_id, self.path))

    def save(self):
        # update meta
        for i in range(0, self.chunk_per_disk):
            self.meta["chunk_%d" % i] = self.chunk_status[i]

        # write meta
        with open("%s/meta.json" % self.path, "w") as f:
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


class RemoteDisk(SimpleDisk):
    def __init__(self, local, gate, disk_id: int, disk_meta):
        super().__init__(disk_id, disk_meta)
        self.local = local
        self.gate: PeerGate = gate

    def allocate(self):
        if self.local:
            super().allocate()
        else:
            self.gate.remote_allocate(self.disk_id)

    def activate(self):
        if self.local:
            super().activate()
        else:
            self.gate.remote_activate(self.disk_id)

    def save(self):
        if self.local:
            super().save()
        else:
            self.gate.remote_save(self.disk_id)

    def write_chunk(self, chunk_idx, chunk):
        # locate chunk
        if self.local:
            super().write_chunk(chunk_idx, chunk)
        else:
            self.gate.remote_write(self.disk_id, chunk_idx, chunk)

    def read_chunk(self, chunk_idx):
        # locate chunk
        if self.local:
            return super().read_chunk(chunk_idx)
        else:
            return self.gate.remote_read(self.disk_id, chunk_idx)

    # label a chunk as parity chunk, thus it cannot be used for store stripes
    def set_parity(self, chunk_idx):
        if self.local:
            super().set_parity(chunk_idx)
        else:
            self.gate.remote_set_parity(self.disk_id, chunk_idx)

    def set_status(self, chunk_idx, status):
        if self.local:
            super().set_status(chunk_idx, status)
        else:
            self.gate.remote_set_status(self.disk_id, chunk_idx, status)

    def reset(self):
        if self.local:
            super().reset()
        else:
            self.gate.remote_reset(self.disk_id)

    @property
    def available_chunks(self):
        if self.local:
            return super().available_chunks
        else:
            return self.gate.remote_data_available_chunks(self.disk_id)


# Create disks with this class
class DiskFactory(object):
    def __init__(self, config):
        self.config = config
        pass

    def allocate(self):
        pass

    def activate(self):
        pass

    def new_disk(self, disk_id: int, disk_meta = None):
        pass


class SimpleDiskFactory(DiskFactory):
    def __init__(self, config):
        super().__init__(config)

    def allocate(self):
        pass

    def activate(self):
        pass

    def new_disk(self, disk_id: int, disk_meta=None):
        return SimpleDisk(disk_id, disk_meta)


class RemoteDiskFactory(DiskFactory):
    def __init__(self, config):
        super().__init__(config)
        self.peers = config.peers
        self.local = config.local
        # decide which part of disks to hold
        hosts_count = len(self.peers) + 1
        disks_to_distribute = list(range(0, config.stripe_count + config.parity_count))
        slice_len = math.ceil(len(disks_to_distribute)/hosts_count)
        slices = []
        for i in range(0, hosts_count):
            slices.append(disks_to_distribute[i*slice_len:min((i+1)*slice_len, len(disks_to_distribute))])

        all_hosts = self.peers + [self.local]
        all_hosts.sort()
        local_part_idx = 0
        for i in range(0, hosts_count):
            if all_hosts[i] == self.local:
                local_part_idx = i
                break

        self.local_disks = slices[local_part_idx]

        self.disk_host = []
        slice_idx = 0
        for disk_slice in slices:
            for disk in disk_slice:
                self.disk_host.append(all_hosts[slice_idx])
            slice_idx = slice_idx + 1
        # prepare gates
        self.gates = {}
        for peer in self.peers:
            self.gates[peer] = PeerGate(peer)

        # pass config to local and remote responders

        # local
        local_config = {
            "local_disks": self.local_disks,
            "disk_meta": {
                "raid_path": self.config.name,
                "file_size": self.config.file_size,
                "chunk_size": self.config.r,
                "disk_size": self.config.disk_size,
                "chunk_per_disk": self.config.disk_size / self.config.r,
            }
        }
        # remote
        for peer in self.peers:
            peer_config = copy.deepcopy(local_config)
            peer_part_idx = 0
            for i in range(0, hosts_count):
                if all_hosts[i] == peer:
                    peer_part_idx = i
                    break
            peer_config["local_disks"] = slices[peer_part_idx]
            requests.post("http://%s:5000/config" % peer, json.dumps(peer_config))

    def allocate(self):
        sys_config = self.config.dic
        for peer in self.peers:
            peer_config = copy.deepcopy(sys_config)
            peer_config["local"] = peer
            peer_config["peers"] = list((set(sys_config["peers"]) - {peer}) | {self.local})
            requests.post("http://%s:5000/sys/allocate" % peer, json.dumps(peer_config))

    def activate(self):
        pass

    def new_disk(self, disk_id: int, disk_meta=None):
        if disk_id in self.local_disks:
            disk = RemoteDisk(True, None, disk_id, disk_meta)
        else:
            disk = RemoteDisk(False, self.gates[self.disk_host[disk_id]], disk_id, disk_meta)
        return disk