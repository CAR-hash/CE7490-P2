import math

from drive.disk import SimpleDisk
import os
import json
from util.config_util import ConfigObject


class DataObject(object):
    def __init__(self, name, disk_idx, chunk_idx, length):
        self.name = name
        self.disk_idx = disk_idx
        self.chunk_idx = chunk_idx
        self.length = length


class BaseController(object):
    def __init__(self, config):
        self.config = config
        pass

    def create_new_raid(self):
        pass


# all bit function can take bytes or int as input, and returns a bytes object
def bit_wise_add(a, b):
    if type(a) == int:
        a = int.to_bytes(a)
    if type(b) == int:
        b = int.to_bytes(b)

    if len(a) != len(b):
        raise Exception("lengthes of operators of bitwise add must be the same")
    ret = b''
    for i in range(0, len(a)):
        ret = ret + int.to_bytes(a[i] ^ b[i])
    return ret


def byte_multiple_by_g0(b):
    if type(b) == bytes:
        b = int.from_bytes(b)
    x7 = (b & 0x80) >> 7
    return int.to_bytes((b << 1) & 0xFF + x7 + x7 << 2 + x7 << 3 + x7 << 4)


def byte_multiple_by_g0_p(b, power):
    if type(b) == int:
        b = int.to_bytes(b)
    if power == 0:
        return b
    if power == 1:
        return byte_multiple_by_g0(b)
    return byte_multiple_by_g0_p(b, power - 1)


def byte_power(b, power):
    if power == 0:
        return 1
    if power == 1:
        return b
    return byte_multiple(b, byte_power(b, power - 1))


def byte_multiple(v1, v2):
    if type(v2) == bytes:
        v2 = int.from_bytes(v2)
    ret = b'\x00'
    for i in range(0, 8):
        current_digit = v2 & (0x1 << i)
        if current_digit > 0:
            ret = bit_wise_add(ret, byte_multiple_by_g0_p(v1, i))
    return ret


# vector functions take bytes objects as input
def vector_multiple_by_g0(D):
    # g0 is {02}
    ret = b''
    for b in D:
        ret = ret + byte_multiple_by_g0(b)
    return ret


def vector_multiple(v, D):
    # g0 is {02}
    ret = b''
    for b in D:
        ret = ret + byte_multiple(b, v)
    return ret


class SimpleController(BaseController):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.stripe_count = config.stripe_count
        self.parity_count = config.parity_count
        self.chunk_per_disk = math.floor(config.disk_size / config.r)
        self.disk_count = config.stripe_count + config.parity_count
        self.disks = []
        self.data_objs = {}

        # parity
        self.parity_dict = {}
        self.generators = []

        # support two parities
        assert self.parity_count == 2

        # multiplication tables
        self.a_dict = {}
        self.b_dict = {}

    def create_new_raid(self):
        assert self.config.disk_size % self.config.r == 0

        if not os.path.exists(self.config.name):
            os.mkdir(self.config.name)

        disk_meta = {
            "raid_path": self.config.name,
            "file_size": self.config.file_size,
            "chunk_size": self.config.r,
            "disk_size": self.config.disk_size,
            "chunk_per_disk": self.config.disk_size / self.config.r,
        }
        for i in range(0, self.stripe_count + self.parity_count):
            disk = SimpleDisk(disk_id=i, disk_meta=disk_meta)
            disk.allocate()
            self.disks.append(disk)

        # save meta
        with open("%s\\meta.json" % self.config.name, "w") as f:
            f.write(json.dumps(self.config.dic))

        # object data
        with open("%s\\objects.json" % self.config.name, "w") as f:
            temp_dict = {}
            for key in self.data_objs.keys():
                temp_dict[key] = self.data_objs[key].__dict__
            f.write(json.dumps(temp_dict))

        assert self.config.parity_count < 5
        self.generators = [2**i for i in range(0, self.config.parity_count)]
        self.init_multiply_table()

        self.init_parity()

    def activate_raid(self):
        disk_meta = {
            "raid_path": self.config.name,
            "file_size": self.config.file_size,
            "chunk_size": self.config.r,
            "disk_size": self.config.disk_size,
            "chunk_per_disk": self.config.disk_size / self.config.r,
        }
        for i in range(0, self.stripe_count + self.parity_count):
            disk = SimpleDisk(disk_id=i, disk_meta=disk_meta)
            disk.activate()
            self.disks.append(disk)

        with open("%s\\objects.json" % self.config.name, "r") as f:
            temp_dict = json.loads(f.read())
            for key in temp_dict:
                temp_obj = temp_dict[key]
                self.data_objs[key] = DataObject(name=temp_obj['name'], disk_idx=temp_obj['disk_idx'],
                                                 chunk_idx=temp_obj['chunk_idx'], length=temp_obj['length'])

        self.generators = [2**i for i in range(0, self.config.parity_count)]
        self.init_multiply_table()

        self.activate_parity()

    def create_obj(self, name):
        if name in self.data_objs.keys():
            raise Exception("Object %s exists." % name)
        disk_idx = 0
        chunk_idx = 0
        value = bytes(self.config.r)
        created = False
        for disk in self.disks:
            if len(disk.available_chunks) > 0:
                chunk_idx = disk.available_chunks[0]
                disk_idx = disk.disk_id
                chunk_idx = chunk_idx
                disk.write_chunk(chunk_idx, value)
                disk.set_status(chunk_idx, True)
                created = True
                break
        if created:
            self.data_objs[name] = DataObject(name=name, disk_idx=disk_idx, chunk_idx=chunk_idx, length=len(value))
        else:
            raise Exception("No space to create.")

    def write_obj(self, name, value: bytes):
        if name not in self.data_objs.keys():
            raise Exception("Object %s does not exist." % name)

        obj = self.data_objs[name]
        self.data_objs[name].length = len(value)
        disk = self.disks[obj.disk_idx]
        disk.write_chunk(obj.chunk_idx, value)

        self.update_parity([(obj.disk_idx, obj.chunk_idx)])

    def read_obj(self, name):
        if name not in self.data_objs.keys():
            raise Exception("Object %s does not exist." % name)

        obj = self.data_objs[name]
        disk = self.disks[obj.disk_idx]
        value = disk.read_chunk(obj.chunk_idx)

        return value

    def delete_obj(self, name):
        if name not in self.data_objs.keys():
            raise Exception("Object %s does not exist." % name)

        obj = self.data_objs[name]
        disk = self.disks[obj.disk_idx]
        disk.set_status(obj.chunk_idx, False)
        self.data_objs.pop(name)

    @property
    def obj_list(self):
        return self.data_objs.keys()

    def save(self):
        for disk in self.disks:
            disk.save()

        with open("%s\\objects.json" % self.config.name, "w") as f:
            temp_dict = {}
            for key in self.data_objs.keys():
                temp_dict[key] = self.data_objs[key].__dict__
            f.write(json.dumps(temp_dict))

    def init_parity(self):
        for i in range(0, self.chunk_per_disk):
            # locate the parity chunks
            parity_idx = range(self.disk_count - self.config.parity_count - i,
                               self.disk_count - i)
            parity_idx = [((idx + self.disk_count) % self.disk_count, i) for idx in parity_idx]
            self.parity_dict[i] = parity_idx
            for idx in parity_idx:
                # find the corresponding disk
                disk_idx = idx[0]
                # find the corresponding chunk idx in the disk
                chunk_idx = idx[1]

                disk = self.disks[disk_idx]
                disk.set_parity(chunk_idx)

    def activate_parity(self):
        for i in range(0, self.chunk_per_disk):
            # locate the parity chunks
            parity_idx = range(self.disk_count - self.config.parity_count - i,
                               self.disk_count - i)
            parity_idx = [((idx + self.disk_count) % self.disk_count, i) for idx in parity_idx]
            self.parity_dict[i] = parity_idx

    def init_multiply_table(self):
        for x in range(0, 255):
            for y in range(0, 255):
                ret = b'\x00'
                # compute g^y-x
                p = (y - x + 255) % 255
                g_power_y_m_x = byte_power(self.generators[1], y - x)
                # compute a
                self.a_dict[(x, y)] = byte_multiple(g_power_y_m_x, byte_power(int.from_bytes(g_power_y_m_x) + 1, 254))
                # compute b
                self.b_dict[(x, y)] = byte_multiple(byte_power(self.generators, 255 - x), byte_power(int.from_bytes(g_power_y_m_x) + 1, 254))

    # support parity = 2 only
    def repair(self, broken_list):
        if broken_list > self.parity_count:
            raise Exception("Too many broken disks, system cannot rebuild them.")

        for i in range(0, self.chunk_per_disk):
            parity_idx = self.parity_dict[i]

            p_parity_disk = parity_idx[0][0]
            q_parity_disk = parity_idx[1][0]

            broken_parity_disks_idx = []
            intact_parity_disks_idx = []
            for idx in parity_idx:
                if idx[0] in broken_list:
                    broken_parity_disks_idx.append(idx[0])
                else:
                    intact_parity_disks_idx.append(idx[0])
            # repair data stripes
            broken_data_stripes_idx = list(set(broken_list) - set(broken_parity_disks_idx))
            if len(broken_data_stripes_idx) == 1:
                broken_stripe_idx = broken_data_stripes_idx[0]
                if p_parity_disk in intact_parity_disks_idx:
                    # P parity is intact
                    repair_chunk = bytes(self.config.r)
                    for disk_idx in range(0, self.disk_count):
                        if disk_idx != broken_stripe_idx:
                            if disk_idx not in [p_parity_disk, q_parity_disk]:
                                repair_chunk = bit_wise_add(repair_chunk, self.disks[disk_idx].read_chunk(i))
                    repair_chunk = bit_wise_add(repair_chunk, self.disks[p_parity_disk].read_chunk(i))
                    self.disks[broken_stripe_idx].write_chunk(i, repair_chunk)
                else:
                    # use Q parity
                    # compute Qx
                    Q = self.disks[q_parity_disk].read_chunk(i)
                    Q_x = bytes(self.config.r)
                    for disk_idx in range(0, self.disk_count):
                        if disk_idx != broken_stripe_idx:
                            if disk_idx not in [p_parity_disk, q_parity_disk]:
                                Q_x = bit_wise_add(Q_x, vector_multiple(byte_power(self.generators[1], disk_idx % self.disk_count), self.disks[disk_idx].read_chunk(i)))
                    repair_chunk = vector_multiple(byte_power(self.generators[1], 255 - broken_stripe_idx), bit_wise_add(Q, Q_x))
                    self.disks[broken_stripe_idx].write_chunk(i, repair_chunk)
            elif len(broken_data_stripes_idx) == 2:
                x = broken_data_stripes_idx[0]
                y = broken_data_stripes_idx[1]
                P_xy = bytes(self.config.r)
                Q_xy = bytes(self.config.r)
                for disk_idx in range(0, self.disk_count):
                    if disk_idx not in [x, y]:
                        if disk_idx not in [p_parity_disk, q_parity_disk]:
                            P_xy = bit_wise_add(P_xy, self.disks[disk_idx].read_chunk(i))
                            Q_xy = bit_wise_add(Q_xy, vector_multiple(
                                byte_power(self.generators[1], disk_idx % self.disk_count),
                                self.disks[disk_idx].read_chunk(i)))

                P = self.disks[p_parity_disk].read_chunk(i)
                Q = self.disks[q_parity_disk].read_chunk(i)
                repaired_x = bit_wise_add(vector_multiple(self.a_dict[(x, y)], bit_wise_add(P, P_xy)),
                                          vector_multiple(self.b_dict[(x, y)], bit_wise_add(Q, Q_xy)))
                self.disks[x].write_chunk(i, repaired_x)
                repaired_y = bit_wise_add(bit_wise_add(P, P_xy), repaired_x)
                self.disks[y].write_chunk(i, repaired_y)
            # repair parities
        self.update_all()

    def check_failure(self):
        for i in range(0, self.chunk_per_disk):
            parity_idx = self.parity_dict[i]

            p_parity_disk = parity_idx[0][0]
            q_parity_disk = parity_idx[1][0]

            P_now = bytes(self.config.r)

            Q_now = bytes(self.config.r)
            for disk_idx in range(0, self.disk_count):
                if disk_idx not in [p_parity_disk, q_parity_disk]:
                    P_now = bit_wise_add(P_now, self.disks[disk_idx].read_chunk(i))
                    Q_now = bit_wise_add(Q_now, vector_multiple(byte_power(self.generators[1], disk_idx % self.disk_count), self.disks[disk_idx].read_chunk(i)))

            P = self.disks[p_parity_disk].read_chunk(i)
            Q = self.disks[q_parity_disk].read_chunk(i)

            if not (P_now == P and Q_now == Q):
                return False

        return True

    # controller idx: (disk_idx, chunk_idx)
    def update_parity(self, controller_idx):
        # find the groups to update
        group = set()
        for idx in controller_idx:
            group.add(idx[1])
        # update all the groups
        for group_idx in group:
            parity_idx = self.parity_dict[group_idx]
            stripe_disk_idx = list(range(0, self.disk_count))
            for idx in parity_idx:
                stripe_disk_idx.remove(idx[0])

            generator_idx = 0
            for idx in parity_idx:
                parity_disk_idx = idx[0]
                chunk_idx = idx[1]
                disk = self.disks[parity_disk_idx]
                generator = self.generators[generator_idx]
                # prepare the data
                parity_chunk = bytes(self.config.r)
                for stripe in stripe_disk_idx:
                    stripe_disk = self.disks[stripe]
                    current_chunk = stripe_disk.read_chunk(chunk_idx)
                    if generator == 1:
                        parity_chunk = bit_wise_add(parity_chunk, current_chunk)
                    else:
                        parity_chunk = bit_wise_add(parity_chunk, vector_multiple(byte_power(generator, stripe), current_chunk))

                disk.write_chunk(chunk_idx, parity_chunk)
                generator_idx = generator_idx + 1

    def update_all(self):
        # find the groups to update
        # update all the groups
        for group_idx in range(0, self.chunk_per_disk):
            parity_idx = self.parity_dict[group_idx]
            stripe_disk_idx = list(range(0, self.disk_count))
            for idx in parity_idx:
                stripe_disk_idx.remove(idx[0])

            generator_idx = 0
            for idx in parity_idx:
                parity_disk_idx = idx[0]
                chunk_idx = idx[1]
                disk = self.disks[parity_disk_idx]
                generator = self.generators[generator_idx]
                # prepare the data
                parity_chunk = bytes(self.config.r)
                for stripe in stripe_disk_idx:
                    stripe_disk = self.disks[stripe]
                    current_chunk = stripe_disk.read_chunk(chunk_idx)
                    if generator == 1:
                        parity_chunk = bit_wise_add(parity_chunk, current_chunk)
                    else:
                        parity_chunk = bit_wise_add(parity_chunk, vector_multiple(byte_power(generator, stripe % self.disk_count), current_chunk))

                disk.write_chunk(chunk_idx, parity_chunk)
                generator_idx = generator_idx + 1




