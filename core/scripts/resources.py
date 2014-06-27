# The resources module contains resource classes where each class
# provides an appropriate environment in which a data generator
# runs. For examples, 
#   - Environment variables PATH, PYTHONPATH, etc.
#   - Executables for Charmm, NAMD, Amber
#
# This removes machine dependency from data generation and makes
# the same data generation code runnable on different resources.

from abc import ABCMeta, abstractmethod, abstractproperty

#import dockers
#import simulators
import os
import platform
from optparse import OptionParser
import json
import subprocess
import uuid
import time
import threading
import random
import datetime
#import mddb_utils
from multiprocessing import Process
#import impsolv_simulators
#import chunkIt
import cStringIO
import shutil
import sys
import psycopg2
import generator
import imp

# A base resource class provides implementations for resource handling methods such as
# job deployment, data loading, data cleanup.

@staticmethod
def get_res_class(res_configs):
  if not res_configs.get('res_path'):
      mod = imp.load_source('res_mod', res_configs['res_path'])
      res_class        = eval("mod.{res_class}".format(**res_configs))
  else:
      res_class        = eval("{res_class}".format(**res_configs))

  return res_class

class Resource(object):

  generator_options = None

  __metaclass__ = ABCMeta

  io_dir            = 'resource_io'
  local_prefix      = os.path.join(os.getenv('HOME'), 'bigdigsci_data')
  script_dirs       = [os.path.join(os.getenv('BIGDIGSCIPREFIX'), 'bigdigsci')]
  gateway_host      = None

  # Initialize resource configurations, e.g., #nodes per deployments, the name of the
  # job queue (if PBS), etc.
  def __init__(self, user, res_configs, worker_id):

    self.deployment_id = 1

    self.res_configs     = res_configs
    map(lambda x: setattr(self.__class__, *x), self.res_configs.items())

    try:
      gateway_host = self.gateway_host or self.res_name or 'localhost'
    except:
      gateway_host = 'localhost'

    self.user                = user
    self.process_pool        = []

    # a file name for keeping track of the last output sync (rsync) time
    # using it's modified time 
    self.sync_info_fn        = os.path.join('/tmp', gateway_host + '_' + str(uuid.uuid4()))
    # open and close the file to change the modified time
    open(self.sync_info_fn, 'w').close()

  # deployment id counter
  def get_next_deployment_id(self):
    deployment_id = self.deployment_id
    self.deployment_id = self.deployment_id + 1
    return deployment_id 

  # convert the sync period in secs into datetime.timedelta
  def get_sync_period(self):
    return datetime.timedelta(0, self.sync_period)


  # take an integer i; convert it to a string of a 6-digit hex; put a dash '-' every two characters
  # and put a prefix in front.
  # used to convert a deployment id and a run id into a directory name
  @staticmethod
  def int2dirname(dir_prefix, i):
    s = '{0:06x}'.format(i)
    return dir_prefix + '-'.join(s[i:i+2] for i in range(0, len(s), 2))

  @staticmethod
  def get_deployment_name(deployment_id):
    return Resource.int2dirname('d_', deployment_id)


  # get the resource config dictionary by modifying the default resource config dictionary
  # to get appropriate settings for MPI, GPU, normal resource configurations.
  def get_res_config(self, config_name, **kwargs):
    res_config = self.__class__.res_configs.get(config_name)
    if not res_config:
      return dict(self.default_config.items() + kwargs.items())
    return dict(self.default_config.items() + res_config.items() + kwargs.items())

  # open and close the file to change the modified time
  def set_last_sync(self):
    open(self.sync_info_fn, 'w').close()

  # read the modified time as a datetime object
  def get_last_sync(self):
    t = datetime.datetime.fromtimestamp( os.path.getmtime(self.sync_info_fn))
    return t

  # check for completed deployments to free up the deployment process pool
  # to allow more deployments. Returns true if the pool size is smaller than
  # the number of allowed deployments (e.g., the #queue entries limit for PBS)
  def check_deployments(self):
    finished_processes = [p for p in self.process_pool if not p.is_alive()]
    self.process_pool  = [p for p in self.process_pool if p not in finished_processes]

    for p in finished_processes:
      p.join()
    
    return len(self.process_pool) < self.num_deployments

  @staticmethod
  def get_environ():
    raise NotImplementedError( "Should have implemented this" )

  @staticmethod
  def print_environ(d):
    return " ".join(["{0}={1}".format(k,":".join(v)) for k,v in d.items()])

  # Starts a deployment process and insert it into the deployment process pool
  def deploy(self, session_dir, input_data, param_dict):
    print "this is in resource.deploy()"
    deployment_id = self.get_next_deployment_id()
    self.prepare_job_dicts(session_dir, deployment_id, input_data, param_dict)
    p = Process(target = self.deploy_and_wait, args = (session_dir, deployment_id, input_data, param_dict))
    p.start()
    self.process_pool.append(p)

  # Add job running related attributes into each job dictionary
  # mostly the directory structure of where the job executions take place
  # on the resource. This method is resource invarient. There is no need
  # to overide/reimplement this definition.
  def prepare_job_dicts(self, session_dir, deployment_id, job_dicts, param_dict):
    st_io = cStringIO.StringIO()
    for i,d in enumerate(job_dicts):
      print d
      d.update(d.pop('data'))
      run_dir = self.int2dirname('r_', i)
      deployment_dir = self.get_deployment_name(deployment_id)

      d['run_dir'] = run_dir
      d['deployment_dir'] = deployment_dir
      d['deployment_id'] = deployment_id
      d['session_dir'] = session_dir
      d['user'] = self.user

      d['dbname']  = param_dict['dbname']

      local_paths = LocalResource.get_paths()

      d['output_prefix']   = os.path.join(local_paths['resource_prefix'],
                                          local_paths['io_dir'],
                                          session_dir,
                                          deployment_dir,
                                          run_dir)

      d['dest_dir']        = os.path.join(local_paths['resource_prefix'],
                                          d['dbname'])

      ts_str = "'{0}'".format(datetime.datetime.now())
      st_io.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(d['jq_entry_id'],
                                                self.worker_id,
                                                session_dir,
                                                deployment_id,
                                                i,
                                                ts_str
                                               ))

    st_io.seek(0)
    conn = psycopg2.connect(database=param_dict['dbname'])

    '''
    conn = mddb_utils.get_dbconn(param_dict['dbname'],
                                 param_dict['dbuser'],
                                 param_dict['dbhost'],
                                 param_dict['dbpass'])
    '''

    cur = conn.cursor()
    cur.copy_from(st_io, 'JobExecutionRecords', sep= '\t')
    conn.commit()
    conn.close()
    st_io.close()

  @staticmethod
  def get_paths():
    raise NotImplementedError( "Should have implemented this" )

  # Job loading method:
  #   (1) Iterate through a list of job dictionaries to call the appropriate load function
  #       according to the generator
  @staticmethod
  def load(conn, job_data, local_paths):
    print "load function" 
    if not isinstance(job_data, list):
      job_data = [job_data]

    st_io = cStringIO.StringIO()
    for d in job_data:
      print 'loading: ', d['jq_entry_id']
      result_dir = os.path.join(Resource.local_prefix, Resource.io_dir, 
                                d['session_dir'], d['deployment_dir'], d['run_dir'])
      print 'from: ', result_dir
      gen_name   = d.get('generator') or d.get('simulator') or d.get('docker')   
      # TODO does not work, PI_Estimator is not callable
      print Resource.generator_options
      gen_obj = Resource.generator_options[gen_name]()

      ret = gen_obj.load(conn, result_dir, d, local_paths)
      #try:
      #except:
      #  ret = (False, '')

      try:
        alright,path = ret
      except:
        alright = True
        path = ''


      ts_str = "'{0}'".format(datetime.datetime.now())
      st_io.write('{0}\t{1}\t{2}\t{3}\n'.format(d['jq_entry_id'],alright, path, ts_str))
      sys.stdout.flush()

    st_io.seek(0)
    cur = conn.cursor()
    cur.copy_from(st_io, 'JobCompletionRecords', sep= '\t')
    cur.close()
    conn.commit()
    st_io.close()

  # Data Clean Up method
  #   (1) remove the job running directory from the file system
  #   (2) if remote, ssh into the remote resource and remove the job running directory there as well

  def cleanup(self, job_data, local_paths):
    path_dict = self.__class__.get_paths()

    if not isinstance(job_data, list):
      job_data = [job_data]

    l = []
    for d in job_data:
      session_dir = d['session_dir']
      result_dir = os.path.join(Resource.local_prefix, Resource.io_dir, session_dir, d['deployment_dir'], d['run_dir'])
      try:
        shutil.rmtree(result_dir)
      except Exception as e:
        print type(e), e


      result_dir_remote = os.path.join(path_dict['resource_prefix'], 
                                       path_dict['io_dir'],
                                       session_dir,
                                       d['deployment_dir'],
                                       d['run_dir'])
      l.append(result_dir_remote)                                       
    if self.gateway_host:
      Resource.remove_remote_dirs(self.gateway_host, l)
      

  # Probing for an empty process slot ensuring that at any given point in time
  # the device id is unique which is critical for CUDA-based jobs
  @staticmethod
  def wait_avail_device(device_pool):
    while True:
      for device_id, proc in device_pool.items():
        if proc == None:
          return device_id
        elif not proc.is_alive():
          proc.join()
          return device_id
      time.sleep(2)
  

  # Execute n jobs in parallel where n = job_concurrency
  # If the number of jobs is greater than n, then some jobs
  # will have to wait for an empty slot. 
  def execute_jobs_parallel(self, job_data, path_dict):
    device_pool = dict([(i,None) for i in range(self.job_concurrency)])

    for d in job_data:
      device_id = Resource.wait_avail_device(device_pool)
  
      p = Process(target=self.execute_job, args=(d, path_dict, device_id))
      p.start()
      device_pool[device_id] = p

    for p in device_pool.values():
      try:
        p.join()
      except:
        pass

    return


  # execute jobs sequentially
  def execute_job(self, job_data, path_dict, device_id=0):
    
    if not isinstance(job_data, list):
      job_data = [job_data]

    out_data = []
    for input_dict in job_data:
      try:
        num_cores_per_node = self.num_cores_per_node
      except:
        num_cores_per_node = 1

      input_dict['num_cores_per_node'] = num_cores_per_node
      input_dict['device_id'] = device_id
      gen_name  = input_dict.get('generator') or input_dict.get('simulator') or input_dict.get('docker')
      gen_obj = Resource.generator_options[gen_name]()

      run_dict = dict(input_dict.items() + path_dict.items() + self.res_configs.items())

      # specify the joblog file name
      output_prefix = os.path.join(run_dict['resource_prefix'], 
                                   run_dict['io_dir'], 
                                   run_dict['session_dir'], 
                                   run_dict['deployment_dir'],
                                   run_dict['run_dir'])


      if not os.path.exists(output_prefix):
        os.makedirs(output_prefix)

      joblog_fn = os.path.join(output_prefix, 'joblog.txt')
      jobdict_fn = os.path.join(output_prefix, 'jobdict.txt')
      with open(os.path.join(output_prefix, jobdict_fn), 'w') as ofp:
        ofp.write(str(input_dict))

      # save the original values for stdout and stderr 
      actual_stdout = sys.stdout
      actual_stderr = sys.stderr
      with open(joblog_fn,'w') as ofp:
        if self.__class__ != LocalResource:
          # redirect the stout and stderr to the joblog
          sys.stdout = ofp
          sys.stderr = ofp

        #print "THIS IS RUN_DICT: " + str(run_dict)
        out_dict = gen_obj.run(output_prefix, run_dict)
        print "THIS IS OUT_DICT: " + str(out_dict)
        if self.__class__ != LocalResource:
          # reset stdout and stderr to original values
          sys.stdout = actual_stdout
          sys.stderr = actual_stderr

      out_data.append(out_dict)


    if len(out_data) == 1:
      out_data = out_data[0]

    return out_data

class RemoteResource(Resource):

  # RemoteResource
  # Job deployment method (for remote resources)
  #   (1) iterate through a list of job dictionaries (job_dict_list) to preprocess each job
  #       (copying data to appropriate locations) and to find out expected
  #       output files.
  #   (2) Submit the job_dict_list to the associated remote resource
  #   (3) Wait until all expected output files are returend through rsync
  #   (4) Load the data to the database/file system
  #   (5) Clean up the remote and local job running directories

  def deploy_and_wait(self, session_dir, deployment_id, job_dict_list, param_dict):
    deployment_dir = self.get_deployment_name(deployment_id)
    output_data = []
    for d in job_dict_list:
      gen_name  = d.get('generator')
      gen_class = Resource.generator_options[gen_name]
      gen_obj = gen_class()
      gen_obj.preprocess(d, LocalResource.get_paths())
      output_data.append((d['run_dir'],gen_obj.get_output_fns(d)))
      d['user'] = self.user
    print 'job_dict_list:', len(job_dict_list), ' jobs'
    wait_list     = []
    for run_dir,d in output_data:
      for v in d.values():
        if v is not None:
          wait_list.append((run_dir, v))

    path_dict     = self.__class__.get_paths()
    remote_prefix = path_dict['resource_prefix']
    sync_dir      = os.path.join(Resource.io_dir, session_dir)
    try:
      os.makedirs(os.path.join(Resource.local_prefix,  sync_dir))
    except:
      pass

    self.submit(session_dir, deployment_id, job_dict_list)

    #print 'output files: ', wait_list
    while wait_list:
      time.sleep(60)

      ts = datetime.datetime.now()
      t_diff = ts - self.get_last_sync()
      if t_diff > self.get_sync_period():
        self.set_last_sync()
        print ts
        self.__class__.sync_output_dirs(self.__class__.gateway_host, [(sync_dir, None)], Resource.local_prefix, remote_prefix)
        self.set_last_sync()
        #print datetime.datetime.now()
        #print Resource.local_prefix
        #print sync_dir

      wait_list = [(run_dir,fn) for (run_dir,fn) in wait_list
                   if not os.path.isfile(os.path.join(Resource.local_prefix, sync_dir, deployment_dir, run_dir, fn))]
      if wait_list:
        print deployment_dir, 'is still waiting for', zip(*wait_list)[0]
      else:
        print deployment_dir, 'completed'

    conn = mddb_utils.get_dbconn(param_dict['dbname'],
                                 param_dict['dbuser'],
                                 param_dict['dbhost'],
                                 param_dict['dbpass'])


    self.load(conn, job_dict_list, LocalResource.get_paths())
    self.cleanup(job_dict_list, LocalResource.get_paths())

    conn.close()
    
  def compose_exec_cmd(self, compute_node, input_data_fn_remote):
    env_st = Resource.print_environ(self.get_environ())
    script_path = os.path.join(self.res_prefix, 'core/scripts')
    cmd_st = "ssh {0} env {1} python {2}/resources.py --resource {3} --mode execute --jobdata {4}".format(
            compute_node, env_st, script_path,
            self.gateway_host, input_data_fn_remote)

    return cmd_st

  def submit(self, session_dir, deployment_dir, input_data):
    sync_list = []
    for input_dict in input_data:
      input_dict['session_dir'] = session_dir
      gen_name  = input_dict.get('generator')
      gen_class = Resource.generator_options[gen_name]
      gen_obj = gen_class()
      # TODO implement get_sync_info?
      sync_list = sync_list + gen_obj.get_sync_info(input_dict)

    sync_list = list(set(sync_list))

    path_dict            = self.__class__.get_paths()
    session_dir          = os.path.join(Resource.io_dir, session_dir)
    remote_prefix        = path_dict['resource_prefix']
    uid                  = 'job_' + str(uuid.uuid4())
    input_data_fn        = os.path.join(Resource.local_prefix,  session_dir, uid)
    input_data_fn_remote = os.path.join(remote_prefix, session_dir, uid)

    node_list            = self.__class__.compute_nodes
    num_nodes            = len(node_list)

    compute_node         = node_list[0]

    with open(input_data_fn, 'w') as ofp:
      ofp.write(json.dumps(input_data))


    self.__class__.sync_input_dirs(self.__class__.gateway_host, 
                                     sync_list + [(session_dir, "*")], 
                                     Resource.local_prefix, remote_prefix)
    cmd_st = self.__class__.compose_exec_cmd(compute_node, input_data_fn_remote)
    print cmd_st
    subprocess.Popen(cmd_st,
                     shell=True, stdout=subprocess.PIPE,
                     stdin=subprocess.PIPE)

  

  @staticmethod
  def sync_scripts(remote_host, remote_prefix, user, script_dirs):
    for script_dir in script_dirs:
      local_dir  = "{0}/{1}/{2}/".format(os.environ['HOME'], user, script_dir)
      remote_dir = "{0}/{1}/{2}/".format(remote_prefix, 'scripts', script_dir)
      Resource.sync_input(local_dir, remote_host, remote_dir, '*')

  @staticmethod
  def sync_input_dirs(remote_host, sync_list, local_prefix, remote_prefix):
    for dir, pattern in sync_list:
      local_dir  = "{0}/{1}/".format(local_prefix, dir)
      remote_dir = "{0}/{1}/".format(remote_prefix, dir)
      Resource.sync_input(local_dir, remote_host, remote_dir, pattern)

  @staticmethod
  def sync_output_dirs(remote_host, sync_list, local_prefix, remote_prefix):
    for dir, pattern in sync_list:
      local_dir  = "{0}/{1}/".format(local_prefix, dir)
      remote_dir = "{0}/{1}/".format(remote_prefix, dir)
      Resource.sync_output(local_dir, remote_host, remote_dir, pattern)

  @staticmethod
  def remove_remote_dirs(remote_host, remote_dirs):
    cmd_st = "ssh {0} rm -rf {1}".format(remote_host, ' '.join(remote_dirs))
    print cmd_st
    subprocess.Popen(cmd_st, shell=True,
                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read()

  @staticmethod
  def sync_input(local_dir, remote_host, remote_dir, pattern):
    cmd_st = "ssh {0} mkdir -p {1}".format(remote_host, remote_dir)
    print cmd_st
    subprocess.Popen(cmd_st, shell=True,
                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read()
    if pattern == None:
      pattern = ""

    cmd_st = "rsync --size-only -avtr {0}/{3} {1}:{2}".format(local_dir, remote_host, remote_dir, pattern)
    print cmd_st
    subprocess.Popen(cmd_st, shell=True,
                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read()

  @staticmethod
  def sync_output(local_dir, remote_host, remote_dir, pattern):
    cmd_st = "mkdir -p {0}".format(local_dir)
    print cmd_st
    subprocess.Popen(cmd_st, shell=True,
                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read()

    if pattern == None:
      pattern = ""
    cmd_st = "rsync --size-only -avtr {1}:{2}/{3} {0}".format(local_dir, remote_host, remote_dir, pattern)
    print cmd_st
    subprocess.Popen(cmd_st, shell=True,
                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read()

#=====================================================================================

class LocalResource(Resource):
  
  default_config = { 'num_nodes': 1,
                     'job_concurrency': 1,
                     'num_deployments': 30,
                   }

  res_configs = { 'gpu': {'job_concurrency':  1, 'num_deployments': 8},
                  'dev': {'job_concurrency':  1, 'num_deployments': 1},
                }
  
  @staticmethod
  def get_paths():
    path_dict = \
      {
       'local_prefix':    Resource.local_prefix,
       'io_dir':          Resource.io_dir,
       'resource_prefix': Resource.local_prefix,
       'template_prefix': os.path.join('/home/{0}'.format(os.environ['USER']), '{0}/mddb/templates'),
       'template_link':   os.path.join('/home/{0}'.format(os.environ['USER']), '{0}/mddb/templates'),
      }

    return path_dict

  # overiding the remote deployment definition
  # The local deployment executes the generator directly in this method
  # Since the call is blocking, there is no need for probing for expected output files
  # like the remote deployment counterpart.
  def deploy_and_wait(self, session_dir, deployment_id, input_data, param_dict):
    deployment_dir            = self.get_deployment_name(deployment_id)
    abs_deployment_dir        = os.path.join(Resource.local_prefix, Resource.io_dir,  
                                             session_dir, deployment_dir)

    if not os.path.exists(abs_deployment_dir):
      os.makedirs(abs_deployment_dir)

    input_data_fn             = os.path.join(abs_deployment_dir, 'data.txt')
    with open(input_data_fn, 'w') as ofp:
      ofp.write(json.dumps(input_data))


    path_dict = LocalResource.get_paths()
    path_dict['template_prefix'] = path_dict['template_prefix'].format(self.user)
    path_dict['template_link']   = path_dict['template_link'].format(self.user)
    
    
    for d in input_data:
      #gen_name  = d.get('generator') or d.get('simulator') or d.get('docker')
      gen_class = Resource.generator_options[d['generator']]
      gen_obj = gen_class()
      gen_obj.preprocess(dict(d.items() + path_dict.items()))
      #print 'input_dict:', d
    #print 'input_data:', input_data
    self.execute_jobs_parallel(input_data, path_dict)
    conn = psycopg2.connect(database=param_dict['dbname'])

    '''
    conn = mddb_utils.get_dbconn(param_dict['dbname'],
                                 param_dict['dbuser'],
                                 param_dict['dbhost'],
                                 param_dict['dbpass'])
    '''
   
    self.load(conn, input_data, path_dict)
    conn.close()

    #self.cleanup(input_data, LocalResource.get_paths())

  @staticmethod
  def get_environ():
    d = { "PYTHONPATH": ["/usr/bin/python2.7"],}
    return d

if __name__ == '__main__':
  param_dict = { "resource": "",
                 "mode":     "",
                 "jobdata":  "",
                 "res_configs_fn": "",
                 "res_config_name": "",
                 "conf": "",
               }

  option_parser = OptionParser()
  add_parser_options(option_parser, param_dict)

  param_dict = parse_param_dict(option_parser)

  with open(param_dict['conf'], 'r') as ifp:
    configs = eval(ifp.read())
    Resource.generator_options = generator.get_gen_opts(configs)

  if param_dict['mode'] == 'execute':
    with open(param_dict['jobdata'], 'r') as ifp:
      data = json.loads(ifp.read())

    try:
      with open(param_dict['res_configs_fn'], 'r') as ifp:
        d = eval(ifp.read)
    except:
      d = {}

    # TODO do that resource lookup thing we did in worker.py
    res_name = self.param_dict['res_name']
    res_configs = self.param_dict['configs']['resources'][res_name]
    res_class = resources.get_res_class(res_configs) 
    res_configs['res_name'] = res_name
    res_obj = res_class(self.param_dict.get('user')
                        res_configs,
                        self.param_dict['worker_id'])

    res_paths = res_obj.__class__.get_paths()
    print 'res_paths', res_paths
    r.execute_jobs_parallel(data, res_paths)
  else:
    test_dockers()
