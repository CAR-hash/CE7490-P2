import json
import requests


class PeerGate(object):
    def __init__(self, peer_address):
        self.peer_address = peer_address

    def remote_allocate(self, disk_id):
        r = requests.post("http://%s:5000/allocate/%d" % (self.peer_address, disk_id))
        assert r.status_code == 200

    def remote_activate(self, disk_id):
        r = requests.post("http://%s:5000/activate/%d" % (self.peer_address, disk_id))
        assert r.status_code == 200

    # request for a peer
    def remote_read(self, disk_id, chunk_id):
        r = requests.get("http://%s:5000/read/%d/%d" % (self.peer_address, disk_id, chunk_id))
        assert r.status_code == 200
        return r.content

    def remote_data_available_chunks(self, disk_id):
        r = requests.get("http://%s:5000/available-chunks/%d" % (self.peer_address, disk_id))
        assert r.status_code == 200
        chunks_str = str(r.content, encoding='utf-8')
        if chunks_str == '':
            return []
        return [int(chunk_id) for chunk_id in chunks_str.split(',')]

    def remote_write(self, disk_id, chunk_id, value):
        r = requests.post("http://%s:5000/write/%d/%d" % (self.peer_address, disk_id, chunk_id), data=value)
        assert r.status_code == 200

    def remote_save(self, disk_id):
        r = requests.post("http://%s:5000/save/%d" % (self.peer_address, disk_id))
        assert r.status_code == 200

    def remote_set_parity(self, disk_id, chunk_id):
        r = requests.post("http://%s:5000/set-parity/%d/%d" % (self.peer_address, disk_id, chunk_id))
        assert r.status_code == 200

    def remote_set_status(self, disk_id, chunk_id, status):
        r = requests.post("http://%s:5000/set-status/%d/%d/%d" % (self.peer_address, disk_id, chunk_id, status))
        assert r.status_code == 200

    def remote_reset(self, disk_id):
        r = requests.post("http://%s:5000/reset/%d" % (self.peer_address, disk_id))
        assert r.status_code == 200
