import sys
import re
import shlex
from subprocess import Popen
import subprocess
import os

from collections import OrderedDict
from ansible.module_utils.basic import *
from ast import literal_eval

try:
    import xml.etree.cElementTree as ElementTree
except ImportError:
    import xml.etree.ElementTree as ElementTree


opt= """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cliOutput>
  <opRet>0</opRet>
  <opErrno>0</opErrno>
  <opErrstr/>
  <volInfo>
    <volumes>
      <volume>
        <name>vol_1</name>
        <id>8a53ad2b-5678-4216-b92f-44928f91726a</id>
        <status>1</status>
        <statusStr>Started</statusStr>
        <snapshotCount>0</snapshotCount>
        <brickCount>3</brickCount>
        <distCount>3</distCount>
        <stripeCount>1</stripeCount>
        <replicaCount>3</replicaCount>
        <arbiterCount>0</arbiterCount>
        <disperseCount>0</disperseCount>
        <redundancyCount>0</redundancyCount>
        <type>2</type>
        <typeStr>Replicate</typeStr>
        <transport>0</transport>
        <xlators/>
        <bricks>
          <brick uuid="c0ed0b7a-695d-4645-a0e9-5878314932b1">10.70.47.68:/var/lib/heketi/mounts/vg_7fcd66fcfe10407a2c16f4283cf8611d/brick_30a2dd4dd06f50591769b84fce57ab32/brick<name>10.70.47.68:/var/lib/heketi/mounts/vg_7fcd66fcfe10407a2c16f4283cf8611d/brick_30a2dd4dd06f50591769b84fce57ab32/brick</name><hostUuid>c0ed0b7a-695d-4645-a0e9-5878314932b1</hostUuid><isArbiter>0</isArbiter></brick>
          <brick uuid="775b59ea-c9c0-4cab-bf99-b280bbfdb775">10.70.46.153:/var/lib/heketi/mounts/vg_f6750523fe102519c3b6e1b3c01d5d57/brick_2608554e90f7501028c9394f08dca307/brick<name>10.70.46.153:/var/lib/heketi/mounts/vg_f6750523fe102519c3b6e1b3c01d5d57/brick_2608554e90f7501028c9394f08dca307/brick</name><hostUuid>775b59ea-c9c0-4cab-bf99-b280bbfdb775</hostUuid><isArbiter>0</isArbiter></brick>
          <brick uuid="fadb7137-b4b3-4b9b-8179-a528e3b1bf35">10.70.46.21:/var/lib/heketi/mounts/vg_c0dd9aaf7a9db0b3849cfdc6e71fe19a/brick_643a5a917097654230d58a47bdb25bb9/brick<name>10.70.46.21:/var/lib/heketi/mounts/vg_c0dd9aaf7a9db0b3849cfdc6e71fe19a/brick_643a5a917097654230d58a47bdb25bb9/brick</name><hostUuid>fadb7137-b4b3-4b9b-8179-a528e3b1bf35</hostUuid><isArbiter>0</isArbiter></brick>
        </bricks>
        <optCount>3</optCount>
        <options>
          <option>
            <name>nfs.disable</name>
            <value>on</value>
          </option>
          <option>
            <name>transport.address-family</name>
            <value>inet</value>
          </option>
          <option>
            <name>cluster.brick-multiplex</name>
            <value>on</value>
          </option>
        </options>
      </volume>
      <count>1</count>
    </volumes>
  </volInfo>
</cliOutput>
"""

op= """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cliOutput>
  <opRet>0</opRet>
  <opErrno>0</opErrno>
  <opErrstr/>
  <geoRep>
    <volume>
      <name>volume2</name>
      <sessions>
        <session>
          <session_slave>d4008322-10bf-4b44-91b1-781faea89f43:ssh://10.70.43.38::volume1:f8b13afd-9cdd-4b90-98ff-72cbcfc98927</session_slave>
          <pair>
            <master_node>10.70.42.62</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>faulty</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>76812105-2833-487e-ae0b-48c79f41afe0</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
          <pair>
            <master_node>10.70.42.60</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>Stopped</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>a995f104-e2ca-42bd-8642-6c14359dfca3</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
          <pair>
            <master_node>10.70.42.82</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>Stopped</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>d4008322-10bf-4b44-91b1-781faea89f43</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
        </session>
      </sessions>
    </volume>
    <volume>
      <name>volume3</name>
      <sessions>
        <session>
          <session_slave>d4008322-10bf-4b44-91b1-781faea89f43:ssh://10.70.43.38::volume1:f8b13afd-9cdd-4b90-98ff-72cbcfc98927</session_slave>
          <pair>
            <master_node>10.70.42.62</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>Stopped</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>76812105-2833-487e-ae0b-48c79f41afe0</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
          <pair>
            <master_node>10.70.42.60</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>Stopped</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>a995f104-e2ca-42bd-8642-6c14359dfca3</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
          <pair>
            <master_node>10.70.42.82</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>Stopped</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>d4008322-10bf-4b44-91b1-781faea89f43</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
        </session>
      </sessions>
    </volume>
    <volume>
      <name>volume4</name>
      <sessions>
        <session>
          <session_slave>d4008322-10bf-4b44-91b1-781faea89f43:ssh://10.70.43.38::volume5:f8b13afd-9cdd-4b90-98ff-72cbcfc98927</session_slave>
          <pair>
            <master_node>10.70.42.62</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>Stopped</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>76812105-2833-487e-ae0b-48c79f41afe0</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
          <pair>
            <master_node>10.70.42.60</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>Active</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>a995f104-e2ca-42bd-8642-6c14359dfca3</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
          <pair>
            <master_node>10.70.42.82</master_node>
            <master_brick>/gluster-bricks/b1/b1</master_brick>
            <slave_user>root</slave_user>
            <slave>ssh://10.70.43.38::volume1</slave>
            <slave_node>N/A</slave_node>
            <status>started</status>
            <crawl_status>N/A</crawl_status>
            <entry>N/A</entry>
            <data>N/A</data>
            <meta>N/A</meta>
            <failures>N/A</failures>
            <checkpoint_completed>N/A</checkpoint_completed>
            <master_node_uuid>d4008322-10bf-4b44-91b1-781faea89f43</master_node_uuid>
            <last_synced>N/A</last_synced>
            <checkpoint_time>N/A</checkpoint_time>
            <checkpoint_completion_time>N/A</checkpoint_completion_time>
          </pair>
        </session>
      </sessions>
    </volume>
  </geoRep>
</cliOutput>
"""
dict_georep={}
info = ElementTree.fromstring(op)
it = info.iter("volume")
georep_dict = {}
try:
    while True:
        volume = it.next()
        # georep_dict[volume.find("name").text] = []
        vol_sessions = volume.iter("sessions")
        try:
            while True:
                vol_session = vol_sessions.next()
                session_it = vol_session.iter("session")
                try:
                    while True:
                        session = session_it.next()
                        session_slave_val = session.find("session_slave").text
                        georep_dict[volume.find("name").text] = session_slave_val
                except StopIteration:
                    pass
        except StopIteration:
            pass
except StopIteration:
    pass
# print georep_dict
master_vols = []
slave_vols = []
for k,v in georep_dict.iteritems():
    s_value = v.split("//")[1].split(":")
    #slave_vol = '::'.join([s_value[0],s_value[2]])
    #master_vol= k
    master_vols.append(k)
    slave_vols.append('::'.join([s_value[0],s_value[2]]))
    #dict_georep['slavevol']=slave_vol
    #dict_georep['mastervol']=master_vol
    # self.module.exit_json(rc=0,msg=dict_georep)
dict_georep["mastervol"] = master_vols
dict_georep["slavevol"] = slave_vols
# print dict_georep


# def VolumeInfo(opt):
root = ElementTree.fromstring(opt)
volumes_dict = {}
for volume in root.findall('volInfo/volumes/volume'):
    value_dict = {}
    value_dict['volumeName'] = volume.find('name').text
    value_dict['replicaCount'] = volume.find('replicaCount').text
    value_dict['volumeType'] = volume.find('typeStr').text.replace('-', '_')
    volumes_dict[value_dict['volumeName']] = value_dict
# print volumes_dict
# return volumes_dict

# def VolumeGeoRepStatus(volumeName, op):
slaves = {}
for volumeName in volumes_dict:
     if "Replicate" in volumes_dict[volumeName]["volumeType"]:
         repCount = int(volumes_dict[volumeName]["replicaCount"])
        #  print repCount
     else:
         repCount = 1
tree = ElementTree.fromstring(op)
volume = tree.find('geoRep/volume')
other_status = ['active', 'initializing']

for session in volume.findall('sessions/session'):
    session_slave = session.find('session_slave').text
    slave = session_slave.split("::")[-1]
    slaves[slave] = {'nodecount': 0,
                     'faulty': 0,
                     'notstarted': 0,
                     'stopped': 0,
                     'passive': 0,
                     'detail': '',
                     'status': 'GeoRepStatus.OK',
                     'name': session_slave.split(":", 1)[1]
                     }
    for pair in session.findall('pair'):
        slaves[slave]['nodecount'] += 1
        status = pair.find('status').text
        # print status
        tempstatus = None
        if "faulty" in status:
            slaves[slave]['faulty'] += 1
            # print tempstatus
        elif "created" in status:
            slaves[slave]['notstarted'] += 1
            tempstatus = "notstarted"
            # print tempstatus
        elif "passive" in status:
            slaves[slave]['passive'] += 1
            tempstatus = "passive"
        elif "stopped" in status:
            slaves[slave]['stopped'] += 1
            tempstatus = "stopped"
        elif status not in other_status:
            tempstatus = status
    if slaves[slave]['faulty'] > 0:
        if repCount > 1:
            if (slaves[slave]['faulty'] + slaves[slave]['passive']
                    > slaves[slave]['nodecount']/repCount):
                slaves[slave]['status'] = "faulty"
            else:
                slaves[slave]['status'] = "partial_faulty"
        else:
            slaves[slave]['status'] = "faulty"
    elif (slaves[slave]['notstarted'] > 0 and
          slaves[slave]['status'] == "ok"):
        slaves[slave]['status'] ="notstarted"
    elif (slaves[slave]['stopped'] > 0 and
          slaves[slave]['status'] == "ok"):
          slaves[slave]['status'] = "Stopped"

    def nested_get(_dict, keys, default=None):
        def _reducer(d,key):
            if isinstance(d,dict):
                return d.get(key,default)
            return default
        return reduce(_reducer, keys, _dict)
    status_list = []
    status_list = {volumeName: {'slaves': slaves}}
    status_value =  nested_get(status_list,['vol_1','slaves','volume1:f8b13afd-9cdd-4b90-98ff-72cbcfc98927','status'])
    # print status_value
    dict_georep["status"]=status_value
    print dict_georep
