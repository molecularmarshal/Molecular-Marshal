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


class Pi_Controller(controller.Controller):
  app_param_dict = {
    'dbname':              'testdb_pi',
    'dbdir':               'database',
    'local_prefix':        os.getenv('BIGDIGSCIPREFIX'),
    'app_prefix':          'apps/pi_mc',
    'app_scriptdir':       'scripts',
    'mode':                '',
    'dep_name':            'default',
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
    #l.append({'hostname':'localhost', 'num_workers': 3})

    l.append({'res_name':'DummyResource', 
              'num_workers': 1,
              'dep_config_name': self.param_dict['dep_name'],
            })

    '''
    l.append({'res_name':'StampedeResource', 
              'num_workers': 1,
              'dep_config_name': self.param_dict['dep_name'],
            })
    '''

    self.setup_workers(l)


  def exec_jobs(self):
    conn = psycopg2.connect(database=self.param_dict['dbname'])
    cur = conn.cursor()
    cur.execute('truncate table jobqueue')
    conn.commit()

    num_jobs = 100

    x_rand_seeds = []
    y_rand_seeds = []
    for i in xrange(0,num_jobs):
      x_rand_seeds.append(('x_rand_seed', "%.20f" % time.time()))

    for i in xrange(0,num_jobs):
      y_rand_seeds.append(('y_rand_seed', "%.20f" % time.time()))
   
    job_dicts =  map(dict, zip(x_rand_seeds, y_rand_seeds))
    num_samples = 100
    for d in job_dicts:
      d['generator']   = 'pi_estimator'
      d['num_samples'] = num_samples
      d['result_fn']   = 'result.txt'
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
  controller = Pi_Controller()
  controller.run()
