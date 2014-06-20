import os, os.path, subprocess, tempfile
import datetime, time
from optparse import OptionParser
import sys
import parser as mddb_parser
import json
import base64
import xml.etree.ElementTree as ET
import cStringIO
import socket
import my_utils
import traceback
from threading import Thread
import re
import math
import random
import platform
import subprocess

import psycopg2
import psycopg2.extras
import uuid
import param_dict_parser

import resources 

class Worker():
  # default parameters for the Worker class
  param_dict = {
    'dbname'           : "",
    'dbhost'           : "", 
    'dbuser'           : "", 
    'dbpass'           : "", 
    'prefix'           : "", 
    'mode'             : "", 
    'worker_id'        : 0,
    'worker_name'      : "",
    'gen_host'         : "",
    'mwd'              : "",
    'user'             : "",
    'avg_seq_jobs'    : 1, # larger of shorter jobs 
    'qname'            : "normal",
    'res_config_name'  : "default",
    'config_file'      : "default",
  }

  # Worker initilization
  def __init__(self):

    # parse command line arguments
    self.param_dict = param_dict_parser.parse(self.param_dict)


    with open(param_dict['config_file'], 'r') as ifp:
      self.configs = eval(ifp.read())

    print self.param_dict

    res_name = 'localhost'
    proc_id = os.getpid();
    conn = psycopg2.connect(database=self.param_dict['dbname'])
    cur = conn.cursor()

    # update the worker entry
    st = "update Workers set worker_name = '{0}', process_id = {1}, resource_name = '{2}' where worker_id = {3}"

    cur.execute(st.format(self.param_dict['worker_name'], 
                          proc_id,
                          res_name,
                          self.param_dict['worker_id'],
                ))
    cur.close()
    conn.commit()
    conn.close()


#    res_class = resources.resource_dict[res_name]
#
#    self.resource = res_class(self.param_dict.get('user'),
#                              self.param_dict.get('res_config_name'),
#                              worker_id = self.param_dict['worker_id'])

    self.resource = resources.LocalResource(self.param_dict.get('user'),
                                            self.param_dict.get('res_config_name'),
                                            worker_id = self.param_dict['worker_id']
                              
    print st
    print self.param_dict

  def process_work(self):
    print "start processing work"

    try:
      gateway_host = self.resource.gateway_host or 'localhost'
    except:
      gateway_host = 'localhost'

      
    # session directory is named after the gateway host appended by a random string for uniqueness
    session_dir    = 's_' + gateway_host + '_' + str(uuid.uuid4())[0:8]

    while True:
      if self.resource.check_deployments():

        conn = psycopg2.connect(database=self.param_dict['dbname'])
        cur = conn.cursor()
        deployment_size = self.resource.job_concurrency * self.param_dict['avg_seq_jobs']
        sql_st = 'select jobqueue_dequeue({0})'.format(deployment_size)
        cur.execute(sql_st)

        try:
          res = cur.fetchone()[0]
          job_dicts = map(eval, res)

          print datetime.datetime.now()
          print res
        except:
          job_dicts = []

        cur.close()
        conn.commit()
        conn.close()

        if job_dicts: # got job, got slot
          self.resource.deploy(session_dir, job_dicts, self.param_dict)
          if not self.resource.__class__ == resources.LocalResource:
            time.sleep(50)

        else: # got job, no slot
          time.sleep(3)

      else: # no slot
        time.sleep(3)
    return

if __name__ == '__main__':
  worker = Worker()
  sys.stdout.flush()
  print "worker updated"
  print "ready to work"
  worker.process_work()
  



