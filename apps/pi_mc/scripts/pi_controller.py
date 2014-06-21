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
    'app_dir':              'bigdigsci/apps/pi_mc',
    'bigdigsci_dir':        'bigdigsci/core',
    'dbname':               'testdb_pi',
    'dbdir':                'database',
    'script_dir':           'scripts',
  }
  
  app_db_files = ['schema.sql']

  def __init__(self):
    self.param_dict = dict(self.param_dict.items() + self.app_param_dict.items())
    self.add_parser_options(self.param_dict)
    self.param_dict = self.parse_param_dict()

  def run(self):
    if self.param_dict['mode'] == 'init':
      self.quit_everything()
      self.init_database(self.app_db_files)
      sys.exit(0)

    if self.param_dict['mode'] == 'setup':
      self.quit_everything()
      l = []
      l.append({'hostname':'localhost', 'num_workers': 3})
      self.setup_workers(l)
      sys.exit(0)


    if self.param_dict['mode'] == 'run':
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

      print x_rand_seeds, y_rand_seeds

      job_dicts =  map(dict, zip(x_rand_seeds, y_rand_seeds))

      num_samples = 100

      for d in job_dicts:
        d['num_samples'] = num_samples
        cur.execute("select jobqueue_insert('{0}')".format(json.dumps(d)))

      cur.close()
      conn.commit()
      conn.close()   


if __name__ == '__main__':
  controller = Pi_Controller()
  controller.run()
