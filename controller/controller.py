import math
import os
import json

from drive.disk import SimpleDisk, RemoteDiskFactory, SimpleDiskFactory

from util.config_util import ConfigObject
import numpy as np


class DataObject(object):
    def __init__(self, name, disk_idx, chunk_idx, length):
        self.name = name
        self.disk_idx = disk_idx
        self.chunk_idx = chunk_idx
        self.length = length


class MutableDataObject(object):
    def __init__(self, name):
        self.name = name
        self.chunk_dict = {}

class BaseController(object):
    def __init__(self, config):
        self.config = config
        pass

    def create_new_raid(self):
        pass


# all bit function can take bytes or int as input, and returns a bytes object
def bit_wise_add(a, b):
    if type(a) == int:
        a = int.to_bytes(a, length=1, byteorder="little")
    if type(b) == int:
        b = int.to_bytes(b, length=1, byteorder="little")

    if len(a) != len(b):
        raise Exception("lengthes of operators of bitwise add must be the same")
    ret = b''
    for i in range(0, len(a)):
        ret = ret + int.to_bytes(a[i] ^ b[i], length=1, byteorder="little")
    return ret


def byte_multiple_by_g0(b):
    if type(b) == bytes:
        b = int.from_bytes(b, byteorder="little")
    x7 = (b & 0x80) >> 7
    if x7 > 0:
        return int.to_bytes(((b << 1) ^ 0x1d) & 0xff, length=1, byteorder="little")
    else:
        return int.to_bytes((b << 1) & 0xff, length=1, byteorder="little")


def byte_multiple_by_g0_p(b, power):
    if type(b) == int:
        b = int.to_bytes(b, length=1, byteorder="little")
    for i in range(0, power):
        b = byte_multiple_by_g0(b)
    return b


def byte_power(b, power):
    if type(b) == int:
        b = int.to_bytes(b, length=1, byteorder="little")
    if power == 0:
        return int.to_bytes(1, length=1, byteorder="little")
    if power == 1:
        return b
    return byte_multiple(b, byte_power(b, power - 1))


def byte_multiple(v1, v2):
    if type(v2) == bytes:
        v2 = int.from_bytes(v2, byteorder="little")
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
        ret = ret + byte_multiple(v, b)
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
        # Q parameter
        self.q_parameters = []
        for i in range(0, self.chunk_per_disk):
            self.q_parameters.append({})

        # support two parities
        assert self.parity_count == 2

        # multiplication tables
        self.a_dict = {}
        self.b_dict = {}

        if config.remote:
            self.factory = RemoteDiskFactory(config)
        else:
            self.factory = SimpleDiskFactory(config)

    def create_new_raid(self):
        assert self.config.disk_size % self.config.r == 0

        if not os.path.exists(self.config.name):
            os.mkdir(self.config.name)

        self.factory.allocate()
        disk_meta = {
            "raid_path": self.config.name,
            "file_size": self.config.file_size,
            "chunk_size": self.config.r,
            "disk_size": self.config.disk_size,
            "chunk_per_disk": self.config.disk_size / self.config.r,
        }
        for i in range(0, self.stripe_count + self.parity_count):
            disk = self.factory.new_disk(i, disk_meta)
            disk.allocate()
            self.disks.append(disk)

        # save meta
        with open("%s/meta.json" % self.config.name, "w") as f:
            f.write(json.dumps(self.config.dic))

        # object data
        with open("%s/objects.json" % self.config.name, "w") as f:
            temp_dict = {}
            for key in self.data_objs.keys():
                temp_dict[key] = self.data_objs[key].__dict__
            f.write(json.dumps(temp_dict))

        assert self.config.parity_count < 5
        self.generators = [2**i for i in range(0, self.config.parity_count)]
        # self.init_multiply_table()

        self.init_parity()

    def activate_raid(self):
        self.factory.activate()
        disk_meta = {
            "raid_path": self.config.name,
            "file_size": self.config.file_size,
            "chunk_size": self.config.r,
            "disk_size": self.config.disk_size,
            "chunk_per_disk": self.config.disk_size / self.config.r,
        }
        for i in range(0, self.stripe_count + self.parity_count):
            disk = self.factory.new_disk(i, disk_meta)
            disk.activate()
            self.disks.append(disk)

        with open("%s/objects.json" % self.config.name, "r") as f:
            temp_dict = json.loads(f.read())
            for key in temp_dict:
                temp_obj = temp_dict[key]
                self.data_objs[key] = DataObject(name=temp_obj['name'], disk_idx=temp_obj['disk_idx'],
                                                 chunk_idx=temp_obj['chunk_idx'], length=temp_obj['length'])

        self.generators = [2**i for i in range(0, self.config.parity_count)]
        self.load_multiply_table()

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

        with open("%s/objects.json" % self.config.name, "w") as f:
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

            data_stripes = list(set(range(0, self.disk_count)) -
                                set([idx[0] for idx in parity_idx]))
            data_stripes.sort()
            for p in range(0, len(data_stripes)):
                self.q_parameters[i][data_stripes[p]] = p

    def activate_parity(self):
        for i in range(0, self.chunk_per_disk):
            # locate the parity chunks
            parity_idx = range(self.disk_count - self.config.parity_count - i,
                               self.disk_count - i)
            parity_idx = [((idx + self.disk_count) % self.disk_count, i) for idx in parity_idx]
            self.parity_dict[i] = parity_idx

            data_stripes = list(set(range(0, self.disk_count)) -
                                set([idx[0] for idx in parity_idx]))
            data_stripes.sort()
            for p in range(0, len(data_stripes)):
                self.q_parameters[i][data_stripes[p]] = p

    def init_multiply_table(self):
        if os.path.exists("%s/mul_table_a.json" % self.config.name) and os.path.exists("%s/mul_table_b.json" % self.config.name):
            return
        for x in range(0, 255):
            for y in range(0, 255):
                ret = b'\x00'
                # compute g^y-x
                p = (y - x + 255) % 256
                g_power_y_m_x = byte_power(self.generators[1], p)
                # compute a
                self.a_dict["%d,%d" % (x, y)] = int.from_bytes(byte_multiple(g_power_y_m_x, byte_power((int.from_bytes(g_power_y_m_x, byteorder="little") + 1) % 256, 254)), byteorder="little")
                # compute b
                self.b_dict["%d,%d" % (x, y)] = int.from_bytes(byte_multiple(byte_power(self.generators[1], 255 - x), byte_power((int.from_bytes(g_power_y_m_x, byteorder="little") + 1) % 256, 254)), byteorder="little")

                print("%d,%d" % (x, y))
        with open("%s/mul_table_a.json" % self.config.name, "w") as f:
            f.write(json.dumps(self.a_dict))

        with open("%s/mul_table_b.json" % self.config.name, "w") as f:
            f.write(json.dumps(self.b_dict))

    def load_multiply_table(self):
        if not (os.path.exists("%s/mul_table_a.json" % self.config.name) and os.path.exists("%s/mul_table_b.json" % self.config.name)):
            self.init_multiply_table()
            self.a_dict.clear()
            self.b_dict.clear()

        with open("%s/mul_table_a.json" % self.config.name) as f:
            temp_dict = json.loads(f.read())
            for key in temp_dict.keys():
                tuple_key = (int(key.split(",")[0]), int(key.split(",")[1]))
                self.a_dict[tuple_key] = temp_dict[key]

        with open("%s/mul_table_b.json" % self.config.name) as f:
            temp_dict = json.loads(f.read())
            for key in temp_dict.keys():
                tuple_key = (int(key.split(",")[0]), int(key.split(",")[1]))
                self.b_dict[tuple_key] = temp_dict[key]

    # support parity = 2 only
    def repair(self, broken_list):
        if len(broken_list) > self.parity_count:
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
                    print(f"Fixed one chunks by P:{repair_chunk}")
                else:
                    # use Q parity
                    # compute Qx
                    Q = self.disks[q_parity_disk].read_chunk(i)
                    Q_x = bytes(self.config.r)
                    for disk_idx in range(0, self.disk_count):
                        if disk_idx != broken_stripe_idx:
                            if disk_idx not in [p_parity_disk, q_parity_disk]:
                                p = byte_power(self.generators[1], self.q_parameters[i][disk_idx])
                                Q_x = bit_wise_add(Q_x, vector_multiple(p, self.disks[disk_idx].read_chunk(i)))
                    repair_chunk = vector_multiple(byte_power(self.generators[1], 255 - self.q_parameters[i][broken_stripe_idx]), bit_wise_add(Q, Q_x))
                    self.disks[broken_stripe_idx].write_chunk(i, repair_chunk)
                    print(f"Fixed one chunks by Q:{repair_chunk}")
            elif len(broken_data_stripes_idx) == 2:
                print("Fixing two stripes...")
                x = broken_data_stripes_idx[0]
                y = broken_data_stripes_idx[1]

                P = self.disks[p_parity_disk].read_chunk(i)
                Q = self.disks[q_parity_disk].read_chunk(i)

                P_xy = bytes(self.config.r)
                Q_xy = bytes(self.config.r)
                for disk_idx in range(0, self.disk_count):
                    if disk_idx not in [x, y]:
                        if disk_idx not in [p_parity_disk, q_parity_disk]:
                            cur_chunk = self.disks[disk_idx].read_chunk(i)
                            P_xy = bit_wise_add(P_xy, cur_chunk)
                            Q_xy = bit_wise_add(Q_xy, vector_multiple(
                                byte_power(self.generators[1], self.q_parameters[i][disk_idx]),
                                cur_chunk))

                x_qp = self.q_parameters[i][x]
                y_qp = self.q_parameters[i][y]

                repaired_x = bit_wise_add(vector_multiple(self.a_dict[(x_qp, y_qp)], bit_wise_add(P, P_xy)),
                                          vector_multiple(self.b_dict[(x_qp, y_qp)], bit_wise_add(Q, Q_xy)))
                self.disks[x].write_chunk(i, repaired_x)
                repaired_y = bit_wise_add(bit_wise_add(P, P_xy), repaired_x)
                self.disks[y].write_chunk(i, repaired_y)
                print(f"Fixed two chunks:{repaired_x}, {repaired_y}")
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
                return True
        return False

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
                        parity_chunk = bit_wise_add(parity_chunk, vector_multiple(byte_power(generator,
                                        self.q_parameters[int(group_idx)][stripe]), current_chunk))

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
                        parity_chunk = bit_wise_add(parity_chunk, vector_multiple(byte_power(generator, self.q_parameters[group_idx][stripe]), current_chunk))

                disk.write_chunk(chunk_idx, parity_chunk)
                generator_idx = generator_idx + 1

    def reset(self):
        self.data_objs.clear()
        for disk in self.disks:
            disk.reset()
        self.init_parity()
        self.save()

    def known_q_tst(self):
        group_idx = 1

        parity_idx = self.parity_dict[group_idx]
        stripe_disk_idx = list(range(0, self.disk_count))
        for idx in parity_idx:
            stripe_disk_idx.remove(idx[0])

        test_chunk_idx = 7
        Q = self.disks[parity_idx[1][0]].read_chunk(group_idx)
        Q_x = bytes(self.config.r)
        for stripe in stripe_disk_idx:
            if stripe != test_chunk_idx:
                cur_chunk = self.disks[stripe].read_chunk(group_idx)
                p = self.q_parameters[group_idx][stripe]
                g_p = byte_power(2, p)
                cur_q_x = vector_multiple(g_p, cur_chunk)
                Q_x = bit_wise_add(Q_x, cur_q_x)
        output_chunk = vector_multiple(byte_power(2, 255 - self.q_parameters[group_idx][test_chunk_idx]), bit_wise_add(Q, Q_x))
        original_chunk = self.disks[test_chunk_idx].read_chunk(group_idx)
        return output_chunk, original_chunk


class MutableController(SimpleController):
    def __init__(self, config):
        super().__init__(config)

    def activate_raid(self):
        self.factory.activate()
        disk_meta = {
            "raid_path": self.config.name,
            "file_size": self.config.file_size,
            "chunk_size": self.config.r,
            "disk_size": self.config.disk_size,
            "chunk_per_disk": self.config.disk_size / self.config.r,
        }
        for i in range(0, self.stripe_count + self.parity_count):
            disk = self.factory.new_disk(i, disk_meta)
            disk.activate()
            self.disks.append(disk)

        with open("%s/objects.json" % self.config.name, "r") as f:
            temp_dict = json.loads(f.read())
            for key in temp_dict:
                temp_obj = temp_dict[key]
                mutable_obj = MutableDataObject(name=temp_obj['name'])
                for key in temp_obj["chunk_dict"].keys():
                    idx = [int(i) for i in key.split(",")]
                    mutable_obj.chunk_dict[(idx[0], idx[1])] = temp_obj["chunk_dict"][key]
                self.data_objs[temp_obj['name']] = mutable_obj

        self.generators = [2**i for i in range(0, self.config.parity_count)]
        self.load_multiply_table()

        self.activate_parity()

    def save(self):
        for disk in self.disks:
            disk.save()

        with open("%s/objects.json" % self.config.name, "w") as f:
            temp_dict = {}
            for key in self.data_objs.keys():
                save_obj = {"name": self.data_objs[key].name}
                save_dict = {}
                for chunk_key in self.data_objs[key].chunk_dict.keys():
                    save_key = "%d,%d" % (chunk_key[0], chunk_key[1])
                    save_dict[save_key] = self.data_objs[key].chunk_dict[chunk_key]
                save_obj["chunk_dict"] = save_dict
                temp_dict[key] = save_obj

            f.write(json.dumps(temp_dict))

    def create_obj(self, name):
        if name in self.data_objs.keys():
            raise Exception("Object %s exists." % name)
        self.data_objs[name] = MutableDataObject(name=name)

    def write_obj(self, name, value: bytes):
        if name not in self.data_objs.keys():
            raise Exception("Object %s does not exist." % name)

        self.data_objs[name].chunk_dict.clear()

        # split value
        chunks_count = math.ceil(len(value) / self.config.r)
        data_slices = []
        for i in range(0, chunks_count):
            data_slices.append(value[i*self.config.r
                               :min((i+1)*self.config.r, len(value))])
        groups_to_update = []

        for data_slice in data_slices:
            created = False
            for disk in self.disks:
                if len(disk.available_chunks) > 0:
                    chunk_idx = disk.available_chunks[0]
                    disk_idx = disk.disk_id
                    chunk_idx = chunk_idx
                    disk.write_chunk(chunk_idx, data_slice)
                    disk.set_status(chunk_idx, True)
                    created = True
                    self.data_objs[name].chunk_dict[(disk.disk_id, chunk_idx)] = len(data_slice)
                    groups_to_update.append((disk_idx, chunk_idx))
                    break
            if not created:
                raise Exception("No space to write object.")

            self.update_parity(groups_to_update)

    def read_obj(self, name):
        if name not in self.data_objs.keys():
            raise Exception("Object %s does not exist." % name)
        obj = self.data_objs[name]

        data_slices = []
        for key in obj.chunk_dict.keys():
            disk_idx = key[0]
            chunk_idx = key[1]
            data_slices.append(self.disks[disk_idx].read_chunk(chunk_idx)[:obj.chunk_dict[key]])

        return b''.join(data_slices)

    def delete_obj(self, name):
        if name not in self.data_objs.keys():
            raise Exception("Object %s does not exist." % name)

        obj = self.data_objs[name]

        for key in obj.chunk_dict.keys():
            disk_idx = key[0]
            chunk_idx = key[1]
            self.disks[disk_idx].set_status(chunk_idx, False)

        self.data_objs.pop(name)
