#!/usr/bin/python
# -*- coding: utf-8 -*-

def gluster_status_vol(self):
    cmd_str="gluster volume info --xml"
    try:
        cmd = Popen(
            shlex.split(cmd_str),
            stdin=open(os.devnull, "r"),
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            close_fds=True
        )
        op, err = cmd.communicate()
        info = ElementTree.fromstring(op)
        it = info.iter("bricks")
        self_heal_dict = {}
        try:
            while True:
                bricks = it.next()
                # georep_dict[volume.find("name").text] = []
                # print volume.find("name").text
                brick_value = bricks.iter("brick")
                try:
                    while True:
                        brick_value = vol_sessions.next()
                        brick_it = brick_value.iter("brick")
                        try:
                            while True:
                                session = session_it.next()
                                session_slave_val = session.find("session_slave").text
                                # print (session.find("session_slave").text).split("//")
                                georep_dict[volume.find("name").text] = session_slave_val
                        except StopIteration:
                            pass
                except StopIteration:
                    pass
        except StopIteration:
            pass
