import os

from flask import Flask
from flask import request
from gevent import pywsgi
import json

from disk import SimpleDisk

app = Flask(__name__)

sys_path = ""
disk_list = {}
config = None


@app.route("/")
def hello_world():
    global key1
    print(key1)
    return "h"


@app.route("/config", methods=['POST'])
def init_config():
    global config
    global disk_list
    config = json.loads(str(request.get_data(), encoding='utf-8').replace("'", '"'))
    disk_idx_list = config["local_disks"]
    disk_meta = config["disk_meta"]
    for disk_idx in disk_idx_list:
        disk = SimpleDisk(disk_id=disk_idx, disk_meta=disk_meta)
        disk_list[disk.disk_id] = disk
    return "good"


# prepare sys folder, meta.json, and objects.json
# the config json passed here should be the sys config
@app.route("/sys/allocate", methods=['POST'])
def allocate_sys():
    global config
    global disk_list
    ##os.chdir("..")
    config_str = str(request.get_data(), encoding='utf-8').replace("'", '"')
    config = json.loads(config_str)
    if not os.path.exists(config["name"]):
        os.mkdir(config["name"])

    with open("%s/meta.json" % config["name"], "w") as f:
        f.write(config_str)

    open("%s/objects.json" % config["name"], "w")
    return "good"

@app.route("/allocate/<disk_idx>", methods=['POST'])
def allocate(disk_idx):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    disk_list[disk_idx].allocate()
    disk_list[disk_idx].save()
    return "good"

@app.route("/activate/<disk_idx>", methods=['POST'])
def activate(disk_idx):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    disk_list[disk_idx].activate()
    disk_list[disk_idx].save()
    return "good"

@app.route("/read/<disk_idx>/<chunk_idx>", methods=['GET'])
# respond for a peer
def respond_read(disk_idx, chunk_idx):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    chunk_idx = int(chunk_idx)
    disk = disk_list[disk_idx]
    return disk.read_chunk(chunk_idx)

@app.route("/available-chunks/<disk_idx>", methods=['GET'])
# respond for a peer
def respond_available_chunks(disk_idx):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    chunks = disk_list[disk_idx].available_chunks
    chunks_str = ','.join([str(chunk_idx) for chunk_idx in chunks])
    return chunks_str


@app.route("/write/<disk_idx>/<chunk_idx>", methods=['POST'])
def respond_write(disk_idx, chunk_idx):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    chunk_idx = int(chunk_idx)
    disk = disk_list[disk_idx]
    disk.write_chunk(chunk_idx, request.get_data())
    disk.save()
    return "good"

@app.route("/set-parity/<disk_idx>/<chunk_idx>", methods=['POST'])
def respond_set_parity(disk_idx, chunk_idx):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    chunk_idx = int(chunk_idx)
    disk = disk_list[disk_idx]
    disk.set_parity(chunk_idx)
    disk.save()
    return "good"


@app.route("/set-status/<disk_idx>/<chunk_idx>/<status>", methods=['POST'])
def respond_set_status(disk_idx, chunk_idx, status):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    disk = disk_list[disk_idx]
    sta = False
    if int(status) > 0:
        sta = True
    disk.set_status(chunk_idx, status=sta)
    disk.save()
    return "good"

@app.route("/save/<disk_idx>", methods=['POST'])
def respond_save(disk_idx):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    disk = disk_list[disk_idx]
    disk.save()
    return "good"

@app.route("/reset/<disk_idx>", methods=['POST'])
def respond_reset(disk_idx):
    global disk_list
    #os.chdir("..")
    disk_idx = int(disk_idx)
    disk = disk_list[disk_idx]
    disk.reset()
    disk.save()
    return "good"

if __name__ == "__main__":
    server = pywsgi.WSGIServer(('0.0.0.0',5000),app)
    server.serve_forever()