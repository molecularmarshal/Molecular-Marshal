from optparse import OptionParser

import datetime, time

import subprocess
import os
import sys
import random
import socket
import pprint
import json
import itertools
import inspect
import glob
import controller

import ast
import math

from multiprocessing import Process
import psycopg2
import psycopg2.extras

class Amber_Controller(controller.Controller):
  app_param_dict = {
    'dbname':              'testdb_bpti',
    'dbdir':               'database',
    'local_prefix':        os.getenv('BIGDIGSCIPREFIX'),
    'app_scriptdir':       'scripts',
    'mode':                '',
    'dep_name':            'default',
    'app_dir':             'apps/amber_bpti',
  }
  
  app_db_files = ['schema.sql']

  def __init__(self):
    self.param_dict = dict(self.param_dict.items() + self.app_param_dict.items())
    self.add_parser_options(self.param_dict)
    self.param_dict = self.parse_param_dict()


  def init(self):
    self.quit_everything()
    self.init_database(self.app_db_files)


  def setup(self):
    self.quit_everything()
    l = []

    '''    
    l.append({'res_name':'LocalResource', 
              'num_workers': 3,
              'dep_config_name': self.param_dict['dep_name']})
    '''
    '''
    l.append({'res_name':'DummyResource', 
              'num_workers': 1,
              'dep_config_name': self.param_dict['dep_name'],
            })
        
    '''

    l.append({'res_name':'StampedeResource', 
              'num_workers': 1,
              'dep_config_name': self.param_dict['dep_name'],
            })
    

    self.setup_workers(l)


  def exec_jobs(self):
    conn = psycopg2.connect(database=self.param_dict['dbname'])
    cur = conn.cursor()
    cur.execute('truncate table jobqueue')
    conn.commit()

    l = [(1,1),(1,2)]
    l = map(lambda (trj_id,t): {'trj_id': "{0:03d}".format(trj_id), 't': t}, l)

    default_params = {'nstep_simulation': 50000000,
                      'trj_save_freq': 50000,
                      'data_source': os.path.join(os.getenv('BIGDIGSCIDATA'),'bpti_data'),
                      'generator': 'Amber_Simulator',
                     }

    cur = conn.cursor()
    cur.execute('truncate table jobqueue')
    for d in l:
      d.update(default_params)
      print d
      cur.execute("select jobqueue_insert('{0}')".format(json.dumps(d)))

    cur.close()
    conn.commit()
    conn.close()   


  def run(self):
    if self.param_dict['mode'] == 'init':
      self.init()
    elif self.param_dict['mode'] == 'setup':
      self.setup()
    elif self.param_dict['mode'] == 'exec':
      self.exec_jobs()
    elif self.param_dict['mode'] == 'debug':
      self.init()
      self.setup()
      self.exec_jobs()


if __name__ == '__main__':
  controller = Amber_Controller()
  controller.run()
