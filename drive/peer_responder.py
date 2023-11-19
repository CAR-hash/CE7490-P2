import os

from flask import Flask
from flask import request
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


# prepare sys folder, meta.json, and objects.json
# the config json passed here should be the sys config
@app.route("/sys/allocate", methods=['POST'])
def allocate_sys():
    global config
    global disk_list
    os.chdir("..")
    config_str = str(request.get_data(), encoding='utf-8').replace("'", '"')
    config = json.loads(config_str)
    if not os.path.exists(config["name"]):
        os.mkdir(config["name"])

    with open("%s\\meta.json" % config["name"], "w") as f:
        f.write(config_str)

    open("%s\\objects.json" % config["name"], "w")


@app.route("/allocate/<disk_idx>", methods=['POST'])
def allocate(disk_idx):
    global disk_list
    os.chdir("..")
    disk_list[disk_idx].allocate()
    disk_list[disk_idx].save()


@app.route("/activate/<disk_idx>", methods=['POST'])
def activate(disk_idx):
    global disk_list
    os.chdir("..")
    disk_list[disk_idx].activate()
    disk_list[disk_idx].save()


@app.route("/read/<disk_id>/<chunk_id>", methods=['GET'])
# respond for a peer
def respond_read(disk_id, chunk_id):
    global disk_list
    os.chdir("..")
    disk = disk_list[disk_id]
    return disk.read_chunk(chunk_id)


@app.route("/write/<disk_id>/<chunk_id>", methods=['POST'])
def respond_write(disk_id, chunk_id):
    global disk_list
    os.chdir("..")
    disk = disk_list[disk_id]
    disk.write_chunk(chunk_id, request.get_data())
    disk.save()

@app.route("/set-parity/<disk_id>/<chunk_id>", methods=['POST'])
def respond_set_parity(disk_id, chunk_id):
    global disk_list
    os.chdir("..")
    disk = disk_list[disk_id]
    disk.set_parity(chunk_id)
    disk.save()


@app.route("/set-status/<disk_id>/<chunk_id>/<status>", methods=['POST'])
def respond_set_status(disk_id, chunk_id, status):
    global disk_list
    os.chdir("..")
    disk = disk_list[disk_id]
    sta = False
    if status > 0:
        sta = True
    disk.set_status(chunk_id, status=sta)
    disk.save()


@app.route("/reset/<disk_id>", methods=['POST'])
def respond_reset(disk_id):
    global disk_list
    os.chdir("..")
    disk = disk_list[disk_id]
    disk.reset()
    disk.save()