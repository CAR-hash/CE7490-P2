import json
import requests


class PeerGate(object):
    def __init__(self, peer_address):
        self.peer_address = peer_address

    def remote_allocate(self, disk_id):
        r = requests.post("http://localhost:5000/allocate/%d" % disk_id)
        assert r.status_code == 200

    def remote_activate(self, disk_id):
        r = requests.post("http://localhost:5000/activate/%d" % disk_id)
        assert r.status_code == 200

    # request for a peer
    def remote_read(self, disk_id, chunk_id):
        r = requests.get("http://localhost:5000/read/%d/%d" % (disk_id, chunk_id))
        assert r.status_code == 200
        return r.content

    def remote_write(self, disk_id, chunk_id, value):
        r = requests.post("http://localhost:5000/write/%d/%d" % (disk_id, chunk_id), data=value)
        assert r.status_code == 200

    def remote_set_parity(self, disk_id, chunk_id):
        r = requests.post("http://localhost:5000/set-parity/%d/%d" % (disk_id, chunk_id))
        assert r.status_code == 200

    def remote_set_status(self, disk_id, chunk_id, status):
        r = requests.post("http://localhost:5000/set-status/%d/%d/%d" % (disk_id, chunk_id, status))
        assert r.status_code == 200

    def remote_reset(self, disk_id):
        r = requests.post("http://localhost:5000/reset/%d" % disk_id)
        assert r.status_code == 200
