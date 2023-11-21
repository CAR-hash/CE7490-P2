# CE7490-P2
Project2 of CE7490, a distributed storage system.
To create a new system

```
python raid.py -i config/<config file>
```

There are four existing configurations in the config folder. minimal.json supports the minimal implementation, mutable.json support data objects with arbitrary sizes, huge.json supports a large scale of data stripes and remote.json supports a P2P distributed storage system

You can also custom the system by changing the content of configuration files. 

To activate a system

```
python raid.py -i <system name>
```

For example, use 

```
python raid.py -i minimal
```

To activate a minimal system.  To activate a remote system, you need to change the ip addresses in remote.json and set up flask peer responders on each peer first. 

```
python peer_responder.py
```

Activation will open a console supporting following operations:

```
create <obj name> # create a new object
read <obj name> # read an object
write <obj name> -c <content> # write an object
delete <obj name> # delete an object
list # list all objects
check # check if a failure or corruption occurs
repair <list of break disks> # repair the disks
```

Some tests for fault tolerance are prepared in remote_test.py and repair_test.py. The tests in repair_test.py are designed for huge configuration. You can change the obj_count in the file to 48 and change the configuration files used in the script to use it for minimal and mutable configuration.