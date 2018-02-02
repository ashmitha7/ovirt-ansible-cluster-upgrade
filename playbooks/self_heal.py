#!/usr/bin/python

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: self_heal

short_description: module to wait for self heal to finish in gluster hosts.

version_added: "2.4"

description:
    - "To wait for the self heal process of volumes in a host to complete.


author : Ashmitha Ambastha (@ashmitha7)
'''
import sys
import re
import shlex
from subprocess import Popen
import subprocess
import os

from collections import OrderedDict
from ansible.module_utils.basic import *
from ast import literal_eval

class GlusterProcess(object):
    def __init__(self, module):
        self.module = module
        self.action = self._validated_params('action')
        self.gluster_ops()

    def get_playbook_params(self, opt):
        return self.module.params[opt]

    def _validated_params(self, opt):
        value = self.get_playbook_params(opt)
        if value is None:
            msg = "Please provide %s option in the playbook!" % opt
            self.module.fail_json(msg=msg)
        return value
    #
    # def gluster_ops(self):
    #     self_heal = self._validated_params('self_heal')
    #     if self_heal == 'yes':
    #         self.volume_name = self._validated_params('volume_name')
    #         cmd = self.gluster_action()
    #         rc, out, err = self.run_command('volume heal ' + self.volume_name + ' ' + self.action, cmd)
    #         self.get_output(rc, out, err)

    def gluster_ops(self):
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



    def run_command(self, op, options):
        cmd = self.module.get_bin_path('gluster', True) + options
        return self.module.run_command(cmd + ' ' + op)

    def get_output(self, rc, output, err):
        if not rc:
            self.module.exit_json(rc=rc, stdout=output, changed=1)
        else:
            self.module.fail_json(rc=rc, msg=err)

    def gluster_action(self):
        args = ' '
        if self.action == 'info':
            args += "info"
        return args

def run_module():
        module = AnsibleModule(
            argument_spec=dict(
            action=dict(choices=["info",], required=True),
            self_heal=dict(type='str'),
            volume_name=dict(type='str'),
            volume_info=dict(type='str'),
            ),
        )
    GlusterProcess(module)

def main():
    run_module()

if __name__ == '__main__':
    main()
