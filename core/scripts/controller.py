from optparse import OptionParser

import re
import StringIO

import datetime, time

import subprocess
import os
import sys
import random
import socket
import pprint
import my_utils
import json
import itertools
import inspect
import glob

import parser as mddb_parser
import cStringIO
import psycopg2

import math

from multiprocessing import Process
import resources

# Generic controller class
class Controller(object): 
  sharedacc = 'webapp'
  
  option_parser = OptionParser()

  # default parameter dictionary for command line arguments
  param_dict = {
    'mode'            : '',
    'core_dbdir'      : 'core/database',
    'core_scriptdir'  : 'core/scripts',
    'template_dir'   : 'templates',
    'dbname'          : ('testdb',
                         'name of the database'),
    'dbhost'          : 'localhost',
    'dbuser'          : os.getenv('USER'),
    'pythonpaths'     : os.getenv('PYTHONPATH') or '',
    'worker'          : 'worker.py',
    'conf'            : 'configs.txt',
  }
 
  # generic __init__ method which parses command line parameters
  def __init__(self, additional_dict=None):
    if additional_dict != None:
      self.param_dict = dict(self.param_dict.items() + self.additional_dict.items())
    self.add_parser_options(self.param_dict)
    self.param_dict = self.parse_param_dict()
    sys.stdout.flush()


  @staticmethod
  def get_job_dicts(default_dict, range_dicts):
    if ange_dicts == []:
      return [default_dict]

    res = []
    for r in range_dicts:
      list_of_list_of_pairs = []
      for k,v in r.items():
        pairs = []
        for a in v:
          pairs.append((k,a))
        list_of_list_of_pairs.append(pairs)

      new_res = map(lambda x: dict(default_dict.items() + dict(x).items()),
                    list(itertools.product(*list_of_list_of_pairs)))
      res = res + new_res
    return res   

  def add_parser_options(self,option_dict):
    for opt_name, default_val in option_dict.iteritems():
      try:
        default_val,comment = default_val
      except:
        comment = opt_name
        pass

      self.option_parser.add_option(
        "--"+opt_name, 
        type=type(default_val), dest=opt_name,
        default=default_val,
        help=comment, metavar="#"+opt_name.upper())

  #does the actual parsing of parameters
  def parse_param_dict(self):
    options,args = self.option_parser.parse_args()
    return dict(vars(options).items())
    

  # database init function:
  #   (1) try to drop the current database and to create a new one 
  #   (2) load schema files specified in 'init_files
  # call this method when you want to create a new database or wipe an existing one
  def init_database(self, init_files):
    dbname = self.param_dict['dbname']
    print dbname
    bigdigsci_prefix = os.getenv('BIGDIGSCIPREFIX')
    if not bigdigsci_prefix:
      bigdigsci_prefix = os.getenv('HOME')

    dbdir  = os.path.join(bigdigsci_prefix,
                          self.param_dict['app_dir'],
                          self.param_dict['dbdir'])

    core_dbdir = os.path.join(bigdigsci_prefix,
                              self.param_dict['core_dbdir'])

    p = my_utils.run_cmd("dropdb {0}".format(dbname))
    p = my_utils.run_cmd("createdb {0}".format(dbname))

    my_utils.load_sql(os.path.join(core_dbdir,'job_control.sql'), dbname)

    for f in init_files:
      my_utils.load_sql(os.path.join(dbdir,f), dbname)
 

  # iterates through the 'Workers' table and terminates all
  # existing worker screen sessions
  def quit_existing_workers(self):
    conn = psycopg2.connect(database=self.param_dict['dbname'])
    cur = conn.cursor()
    cur.execute("select worker_name from Workers")
    for rec in cur:
      gw_name = rec[0]
      my_utils.run_cmd("screen -X -S {0} quit".format(gw_name))
  
    cur.execute("truncate Workers cascade")
    cur.execute("select setval((select pg_get_serial_sequence('workers', 'worker_id')), 1, false)")
    conn.commit()
    cur.close()
    conn.close()
  

  # start a worker screen session (each worker screen session runs a worker process) 
  # according to the parameters specified in self.param_dict
  def start_workers(self, conn, res_name, worker_id, worker_name, dep_config_name):
    worker  = os.path.join(os.getenv('BIGDIGSCIPREFIX'), 
                           self.param_dict['core_scriptdir'],
                           self.param_dict['worker'])
    
    #print os.environ
    sys.stdout.flush()

    args =       """--worker_name {0} """.format(worker_name)+\
                 """--worker_id {0} """.format(worker_id)+\
                 """--res_name {0} """.format(res_name)+\
                 """--dep_config_name {0} """.format(dep_config_name)+\
                 """--dbname {dbname} --dbhost {dbhost} --dbuser {dbuser} --conf {conf}""".format(**self.param_dict)
  
    # start a screen here
    template  =  """screen -S {0} -d -m """.format(worker_name)+\
                 """env PYTHONPATH={pythonpaths} GMX_MAXBACKUP=-1  """ +\
                 """PATH={PATH} """.format(**os.environ) +\
                 """python """ + worker + """ """ + args
  
    my_utils.run_cmd(template.format(**self.param_dict))
 
  # make sure that the corresponding remote resource has up-to-date scripts
  def prepare_resource(self, res_configs, gateway_host):
    if res_configs['res_name'] != 'LocalResource':
      app_path = os.path.join(self.param_dict['local_prefix'],
                              self.param_dict['app_dir'])
      res_class = resources.get_res_class(app_path, res_configs)
      remote_prefix = res_configs['res_prefix']
      # sync core scripts
      res_class.sync_scripts(gateway_host, 
                             remote_prefix, 
                             os.getenv('BIGDIGSCIPREFIX'),
                             self.param_dict['core_scriptdir'])
      
      # sync app scripts 
      app_script_prefix = os.path.join(self.param_dict['app_dir'],
                                       self.param_dict['app_scriptdir'])
      
      res_class.sync_scripts(gateway_host, 
                             remote_prefix, 
                             self.param_dict['local_prefix'],
                             app_script_prefix)
      
      # sync templates
      templates_prefix = os.path.join(self.param_dict['app_dir'],
                                      self.param_dict['template_dir'])

      res_class.sync_scripts(gateway_host, 
                             remote_prefix, 
                             self.param_dict['local_prefix'],
                             templates_prefix)


  # iterates through a list 'l' of workers and starts worker processes
  # there are three type of resources: localhost, localcluster-remotehost,
  # and remote-resource. For the first two, the worker screen session
  # runs on the specified resource 'res_name' and the data is also generated
  # on the woker host. For remote-resource, the worker screen session is
  # running on localhost but the data is generated on the remote resource.
  def setup_workers(self, l):
    conn = psycopg2.connect(database=self.param_dict['dbname'])
    cur  = conn.cursor()

    with open(self.param_dict['conf'], 'r') as ifp:
      configs = eval(ifp.read())
   

    for d in l:
      res_name = d['res_name']

      res_configs = configs['resources'][res_name]
      gateway_host = configs['resources'][res_name]['res_host']
      
      res_configs['res_name'] = res_name
      if res_name == "LocalResource":
        res_configs['res_prefix'] = self.param_dict['local_prefix']
      self.prepare_resource(res_configs, gateway_host)
      for i in xrange(0,d['num_workers']):
        cur.execute("select workers_insert('{0}');".format(res_name))
        worker_id = cur.fetchone()[0]
        worker_name = "{1}_worker_{0}".format(res_name,worker_id)
        
        # do block assignments here
        self.start_workers(conn, res_name, worker_id, worker_name, d['dep_config_name'])

    cur.close()
    conn.commit()
    conn.close()
  
  def quit_everything(self):
    try:
      self.quit_existing_workers()
      # quit the control loop too if implemented
    except:
      pass
