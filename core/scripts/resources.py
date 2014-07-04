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
import generator
import imp
import param_dict_parser

try:
  import psycopg2
except:
  pass

# A base resource class provides implementations for resource handling methods such as
# job deployment, data loading, data cleanup.

def get_res_class(prefix, res_configs):
  if res_configs.get('res_path'):
      print os.path.join(prefix, res_configs['res_path'])
      mod = imp.load_source('res_mod', os.path.join(prefix, res_configs['res_path']))
      res_class = eval("mod.{res_class}".format(**res_configs))
  else:
      res_class = eval("{res_class}".format(**res_configs))

  return res_class

class Resource(object):

  __metaclass__ = ABCMeta

  # Initialize resource configurations, e.g., #nodes per deployments, the name of the
  # job queue (if PBS), etc.
  def __init__(self, user, res_configs, worker_id, 
               gen_opts, dep_config_name, app_dir, local_prefix, is_local=True):
   
    self.worker_id         = worker_id
    self.local_prefix      = local_prefix
    self.deployment_id     = 0
    self.generator_options = gen_opts
    self.res_configs       = res_configs
    self.gateway_host      = self.res_configs['res_host']
    self.dep_config_name   = dep_config_name
    
    self.io_dir            = self.res_configs['io_dir']
    self.res_host          = self.res_configs['res_host']

    if self.res_host == "localhost":
      self.resource_prefix = self.local_prefix
      if self.res_configs.get('deployment_configs') == None:
        self.dep_config = LocalResource.default_config 
    else: 
      self.resource_prefix = self.res_configs['res_prefix']
    
    if not hasattr(self, 'dep_config'):
      
      if is_local:
        dep_config_path = os.path.join(os.getenv('BIGDIGSCIPREFIX'),
                                       app_dir,
                                       self.res_configs['deployment_configs'])
      else :
        dep_config_path = os.path.join(self.resource_prefix,
                                       app_dir,
                                       self.res_configs['deployment_configs'])

      dep_configs = Resource.parse_dep_config(dep_config_path)
      self.dep_config = dep_configs[dep_config_name]
   
    map(lambda (k, v): setattr(self, k, v), self.dep_config.items())
    self.local_prefix      = os.path.join(os.getenv('HOME'), local_prefix)
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
  
  # parse the dep_config.txt
  @staticmethod
  def parse_dep_config(dep_config_path):
    with open(dep_config_path, 'r') as ifp:
      dep_configs = eval(ifp.read())

    default_config = dep_configs['default']
    custom_configs = dep_configs['customized']

    configs = dict(map(lambda (k,v): (k,dict(default_config.items() + v.items())), 
               custom_configs.items()))

    configs['default'] = default_config
    return configs


  # get the resource config dictionary by modifying the default resource config dictionary
  # to get appropriate settings for MPI, GPU, normal resource configurations.
  def get_res_config(self, config_name, **kwargs):
    res_config = self.res_configs.get(config_name)
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

  def get_environ(self):
    raise NotImplementedError( "Should have implemented this" )

  @staticmethod
  def print_environ(d):
    return " ".join(["{0}={1}".format(k,":".join(v)) for k,v in d.items()])

  # Starts a deployment process and insert it into the deployment process pool
  def deploy(self, session_dir, input_data, param_dict):
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

      local_paths = self.get_paths()

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
    
    cur = conn.cursor()
    cur.copy_from(st_io, 'JobExecutionRecords', sep= '\t')
    conn.commit()
    conn.close()
    st_io.close()

  def get_paths(self):
    raise NotImplementedError( "Should have implemented this" )

  # Job loading method:
  #   (1) Iterate through a list of job dictionaries to call the appropriate load function
  #       according to the generator
  def load(self, conn, job_data, local_paths):
    print "load function" 
    if not isinstance(job_data, list):
      job_data = [job_data]

    st_io = cStringIO.StringIO()
    for d in job_data:
      print 'loading: ', d['jq_entry_id']
      result_dir = os.path.join(self.local_prefix, self.io_dir, 
                                d['session_dir'], d['deployment_dir'], d['run_dir'])
      print 'from: ', result_dir
      gen_name   = d.get('generator') or d.get('simulator') or d.get('docker')   
      print self.generator_options
      gen_obj = self.generator_options[gen_name]()

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
    path_dict = self.get_paths()

    if not isinstance(job_data, list):
      job_data = [job_data]

    l = []
    for d in job_data:
      session_dir = d['session_dir']
      result_dir = os.path.join(self.local_prefix, self.io_dir, session_dir, d['deployment_dir'], d['run_dir'])
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
      self.remove_remote_dirs(self.gateway_host, l)
      

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
      gen_name  = input_dict.get('generator')


      #gen_obj = Resource.generator_options[gen_name]()
      gen_obj = self.generator_options[gen_name]()
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

        out_dict = gen_obj.run(output_prefix, run_dict)
        if self.__class__ != LocalResource:
          # reset stdout and stderr to original values
          sys.stdout = actual_stdout
          sys.stderr = actual_stderr

      out_data.append(out_dict)


    if len(out_data) == 1:
      out_data = out_data[0]

    return out_data


#=============================== RemoteResource ======================================#

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
    res_name = param_dict['res_name'] 
    deployment_dir = self.get_deployment_name(deployment_id)
    output_data = []
    for d in job_dict_list:
      gen_name  = d.get('generator')
      gen_class = self.generator_options[gen_name]
      gen_obj = gen_class()
      gen_obj.preprocess(dict(d.items()+ self.get_paths().items()))
      output_data.append((d['run_dir'],gen_obj.get_output_fns(d)))
      d['user'] = self.user
    print 'job_dict_list:', len(job_dict_list), ' jobs'
    wait_list     = []
    for run_dir,d in output_data:
      for v in d.values():
        if v is not None:
          wait_list.append((run_dir, v))

    path_dict     = self.get_paths()
    remote_prefix = path_dict['resource_prefix']
    sync_dir      = os.path.join(self.io_dir, session_dir)
    try:
      os.makedirs(os.path.join(self.local_prefix,  sync_dir))
    except:
      pass
    
    app_dir = param_dict['configs']['app_dir']
    conf = param_dict['conf']
    # absolut path to remote config file
    conf_fn_remote = os.path.join(remote_prefix, app_dir, param_dict['app_scriptdir'], conf)
    worker_id = param_dict['worker_id']
 
    self.submit(session_dir, deployment_dir, deployment_id, res_name,
                job_dict_list, app_dir, conf_fn_remote, worker_id, self.dep_config_name)
    
    while wait_list:
      time.sleep(60)

      ts = datetime.datetime.now()
      t_diff = ts - self.get_last_sync()
      if t_diff > self.get_sync_period():
        self.set_last_sync()
        print ts
        self.__class__.sync_output_dirs(self.gateway_host, [(sync_dir, None)], self.local_prefix, remote_prefix)
        self.set_last_sync()

      wait_list = [(run_dir,fn) for (run_dir,fn) in wait_list
                   if not os.path.isfile(os.path.join(self.local_prefix, sync_dir, deployment_dir, run_dir, fn))]
      if wait_list:
        print deployment_dir, 'is still waiting for', zip(*wait_list)[0]
      else:
        print deployment_dir, 'completed'
    conn = psycopg2.connect(database=param_dict['dbname'])

    self.load(conn, job_dict_list, self.get_paths())
    self.cleanup(job_dict_list, self.get_paths())

    conn.close()

  def compose_exec_cmd(self, compute_node, input_data_fn_remote,
                       conf_fn_remote, res_name, dep_config_name):
    env_st = Resource.print_environ(self.get_environ())
    script_path = os.path.join(self.resource_prefix, 'core/scripts')
    cmd_st = "ssh {0} env {1} python {2}/resources.py --mode execute --jobdata {3} --dep_config_name {4} --conf_fn_remote {5} --res_name {6} ".format(
             compute_node, env_st, script_path, input_data_fn_remote, 
             dep_config_name, conf_fn_remote, res_name)

    return cmd_st

  # submit for RemoteResource
  def submit(self, session_dir, deployment_dir, deployment_id, res_name, 
             input_data, app_dir, conf_fn_remote, worker_id, dep_config_name):
    sync_list = []
    for input_dict in input_data:
      input_dict['session_dir'] = session_dir
      gen_name  = input_dict.get('generator')
      gen_class = self.generator_options[gen_name]
      gen_obj = gen_class()
      # TODO sync scripts when initializing worker 
      # sync_list = sync_list + gen_obj.get_sync_info(input_dict)

    sync_list = list(set(sync_list))

    path_dict            = self.get_paths()
    session_dir          = os.path.join(self.io_dir, session_dir)
    remote_prefix        = path_dict['resource_prefix']
    uid                  = 'job_' + str(uuid.uuid4())
    
    # local and remote path for input data sync
    deployment_path        = os.path.join(self.local_prefix, session_dir, 
                                          deployment_dir)
    deployment_path_remote = os.path.join(remote_prefix, session_dir, deployment_dir)
    
    # file stored input data  
    input_data_fn          = os.path.join(deployment_path, uid)
    input_data_fn_remote   = os.path.join(deployment_path_remote, uid)
   
    # if there is no available compute nodes, gateway_host will be applied to 
    # conducting computing
    try:
      node_list            = self.compute_nodes
      num_nodes            = len(node_list)
      compute_node         = node_list[0]
    except:
      compute_node = self.gateway_host
    
    os.makedirs(deployment_path)

    with open(input_data_fn, 'w') as ofp:
      ofp.write(json.dumps(input_data))

    # sync the input data
    self.__class__.sync_input_dirs(self.gateway_host, 
                                     sync_list + [(os.path.join(session_dir, deployment_dir),'*')], 
                                     self.local_prefix, remote_prefix)
    cmd_st = self.compose_exec_cmd(compute_node, input_data_fn_remote, conf_fn_remote, res_name, dep_config_name)
    print cmd_st
    subprocess.Popen(cmd_st,
                     shell=True, stdout=subprocess.PIPE,
                     stdin=subprocess.PIPE)


  @staticmethod
  def sync_scripts(remote_host, remote_prefix, local_prefix, script_dir):
    local_dir  = os.path.join(local_prefix, script_dir)
    remote_dir = os.path.join(remote_prefix, script_dir)
    RemoteResource.sync_input(local_dir, remote_host, remote_dir, '*')


  @staticmethod
  def sync_input_dirs(remote_host, sync_list, local_prefix, remote_prefix):
    for dir, pattern in sync_list:
      local_dir  = "{0}/{1}/".format(local_prefix, dir)
      remote_dir = "{0}/{1}/".format(remote_prefix, dir)
      RemoteResource.sync_input(local_dir, remote_host, remote_dir, pattern)


  @staticmethod
  def sync_output_dirs(remote_host, sync_list, local_prefix, remote_prefix):
    for dir, pattern in sync_list:
      local_dir  = "{0}/{1}/".format(local_prefix, dir)
      remote_dir = "{0}/{1}/".format(remote_prefix, dir)
      RemoteResource.sync_output(local_dir, remote_host, remote_dir, pattern)


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

#=============================== LocalResource ======================================#

class LocalResource(Resource):

  default_config = { 'num_nodes': 1,
                     'job_concurrency': 1,
                     'num_deployments': 30,
                   }
  res_configs = { 'gpu': {'job_concurrency':  1, 'num_deployments': 8},
                  'dev': {'job_concurrency':  1, 'num_deployments': 1},
                }

  def get_paths(self):
    path_dict = \
      {
       'io_dir':          'resource_io',
       'local_prefix':    self.local_prefix,
       'resource_prefix': self.local_prefix,
      }

    return path_dict

  # overiding the remote deployment definition
  # The local deployment executes the generator directly in this method
  # Since the call is blocking, there is no need for probing for expected output files
  # like the remote deployment counterpart.
  def deploy_and_wait(self, session_dir, deployment_id, input_data, param_dict):
    deployment_dir            = self.get_deployment_name(deployment_id)
    abs_deployment_dir        = os.path.join(self.local_prefix, self.io_dir,
                                             session_dir, deployment_dir)

    if not os.path.exists(abs_deployment_dir):
      os.makedirs(abs_deployment_dir)

    input_data_fn = os.path.join(abs_deployment_dir, 'data.txt')
    with open(input_data_fn, 'w') as ofp:
      ofp.write(json.dumps(input_data))

    path_dict = self.get_paths()
    
    for d in input_data:
      gen_class = self.generator_options[d['generator']]
      gen_obj = gen_class()
      gen_obj.preprocess(dict(d.items() + path_dict.items()))

    self.execute_jobs_parallel(input_data, path_dict)
    conn = psycopg2.connect(database=param_dict['dbname'])
       
    self.load(conn, input_data, path_dict)
    conn.close()
    #self.cleanup(input_data, self.get_paths())

  def get_environ(self):
    d = { "PYTHONPATH": ["/usr/bin/python2.7"],}
    return d


#=============================== PBSResource ======================================#

class PBSResource(RemoteResource):

  # slightly different from the base submit function, this one will compose a job script   
  def submit(self, session_dir, deployment_dir, deployment_id, res_name, 
             input_data, app_dir, conf_fn_remote, worker_id, dep_config_name):
    sync_list = []
    for input_dict in input_data:
      input_dict['session_dir'] = session_dir
      gen_name  = input_dict.get('generator')
      gen_class = self.generator_options[gen_name]
      gen_obj = gen_class()
      # TODO sync scripts when initializing worker 
      # sync_list = sync_list + gen_obj.get_sync_info(input_dict)

    sync_list = list(set(sync_list))

    path_dict            = self.get_paths()
    session_dir          = os.path.join(self.io_dir, session_dir)
    remote_prefix        = path_dict['resource_prefix']
    uid                  = 'job_' + str(uuid.uuid4())
    
    # local and remote path for input data sync
    deployment_path        = os.path.join(self.local_prefix, session_dir, 
                                          deployment_dir)
    deployment_path_remote = os.path.join(remote_prefix, session_dir, deployment_dir)
    
    # file stored input data  
    input_data_fn          = os.path.join(deployment_path, uid)
    input_data_fn_remote   = os.path.join(deployment_path_remote, uid)
   
    # file stored the PBS script
    job_script_fn          = os.path.join(deployment_path, uid + '_script.txt') 
    job_script_fn_remote   = os.path.join(deployment_path_remote, uid + '_script.txt')

    # if there is no available compute nodes, gateway_host will be applied to 
    # conducting computing
    try:
      node_list            = self.compute_nodes
      num_nodes            = len(node_list)
      compute_node         = node_list[0]
    except:
      compute_node = self.gateway_host
    
    os.makedirs(deployment_path)

    with open(input_data_fn, 'w') as ofp:
      ofp.write(json.dumps(input_data))

    with open(job_script_fn, 'w') as ofp:
      ofp.write(self.compose_job_script(input_data_fn_remote, deployment_id, conf_fn_remote, res_name))

    # sync the input data
    self.__class__.sync_input_dirs(self.gateway_host, 
                                   sync_list + [(os.path.join(session_dir, deployment_dir),'*')], 
                                   self.local_prefix, remote_prefix)

    cmd_st = "ssh {0} {1} {2}".format(self.gateway_host, self.submission_cmd, job_script_fn_remote)
    print cmd_st
    subprocess.Popen(cmd_st,
                     shell=True, stdout=subprocess.PIPE,
                     stdin=subprocess.PIPE)

  
  def compose_job_script(self):  
    raise NotImplementedError( "Should have implemented this" )


#=============================== StampedeResource ======================================#

class StampedeResource(PBSResource):

  num_cores_per_node = 16
  submission_cmd = 'sbatch'

  def get_paths(self):
    path_dict = \
      {
       'io_dir':          self.io_dir, 
       'resource_prefix': self.resource_prefix,
      }
    return path_dict

  def get_environ(self):
    d = { "PYTHONPATH": ["/opt/apps/python/2.7.1/modules/lib/python:/opt/apps/python/2.7.1/lib:",
                         "/opt/apps/python/2.7.1/lib/python2.7/",
                         os.path.join(self.resource_prefix,
                                      'core/scripts')
                        ],
         }
    return d

  def compose_job_script(self, input_data_fn_remote, deployment_id, conf_fn_remote, res_name):
    qname = self.qname 
    script_path = os.path.join(self.resource_prefix, 'core/scripts')
    cmd = "/opt/apps/python/epd/7.3.2/bin/python " +\
          "{0}/resources.py " +\
          "--mode execute " +\
          "--jobdata {1} --dep_config_name {2} --conf_fn_remote {3} --res_name {4}"

    script_dict = { "num_cores": self.num_nodes * self.num_cores_per_node,
                    "JOBID": "{0:06x}".format(deployment_id),
                    "QUEUE": qname, "TIME": self.time_limit,
                    "OUTFN": input_data_fn_remote + ".log",
                    "CMD": cmd.format(script_path,input_data_fn_remote, self.dep_config_name, 
                                      conf_fn_remote, res_name),
                  }

    template = "#!/bin/bash\n" +\
               "#SBATCH -J {JOBID}         \n" +\
               "#SBATCH -o {OUTFN}         \n" +\
               "#SBATCH -n {num_cores}     \n" +\
               "#SBATCH -p {QUEUE}         \n" +\
               "#SBATCH -t {TIME}          \n" +\
               "env " + Resource.print_environ(self.get_environ()) + " {CMD}\n"

    return template.format(**script_dict)


#=============================== Main Method ======================================#

if __name__ == '__main__':
  param_dict = { "mode":     "",
                 "jobdata":  "",
                 "app_dir": "",
                 "conf_fn_remote": "",
                 "res_name": "",
                 "worker_id": "",
                 "dep_config_name": "",
               }

  param_dict = param_dict_parser.parse(param_dict)
 

  if param_dict['mode'] == 'execute':
    # Get the input data by reading the input_fn_remote 
    with open(param_dict['jobdata'], 'r') as ifp:
      data = json.loads(ifp.read())
    # Get the resource info by reading the res_fn_remote

    with open(param_dict['conf_fn_remote'], 'r') as ifp:    
      configs = json.loads(ifp.read())

    res_name = param_dict['res_name']
    res_configs = configs['resources'][res_name]
    resource_prefix = res_configs['res_prefix'] 
    app_path = os.path.join(resource_prefix, configs['app_dir'])
    res_class = get_res_class(app_path, res_configs) 

    res_obj = res_class(param_dict.get('user'),
                        res_configs,
                        param_dict['worker_id'],
                        generator.get_gen_opts(app_path, configs['generators']),
                        param_dict['dep_config_name'],
                        configs['app_dir'],
                        configs['local_prefix'],
                        False)

    res_paths = res_obj.get_paths()
    res_obj.execute_jobs_parallel(data, res_paths)
  else:
    test_dockers()
