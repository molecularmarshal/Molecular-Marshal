import resources
import os

class DummyResource(resources.RemoteResource):
  
  def get_paths(self):
    path_dict = \
      {
       'io_dir':          self.io_dir, 
       'resource_prefix': self.resource_prefix,
      }
    return path_dict

  def get_environ(self):
    d = { "PYTHONPATH": [os.path.join(self.resource_prefix,
                                      'core/scripts')]}
    return d
  
  # TODO reimplement submit function for preprocess 
  def submit(self, session_dir, deployment_dir, deployment_id, res_name, 
             input_data, app_dir, conf_fn_remote, worker_id, dep_config_name):

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

    sync_list = []
    for input_dict in input_data:
      input_dcit['input_fn'] = input_data_fn
      input_dict['session_dir'] = session_dir
      gen_name  = input_dict.get('generator')
      gen_class = self.generator_options[gen_name]
      gen_obj = gen_class()
      # TODO sync scripts when initializing worker 
      # sync_list = sync_list + gen_obj.get_sync_info(input_dict)

    sync_list = list(set(sync_list))

    # if there is no available compute nodes, gateway_host will be applied to 
    # conducting computing
    try:
      node_list            = self.compute_nodes
      num_nodes            = len(node_list)
      compute_node         = node_list[0]
    except:
      compute_node = self.gateway_host
    
    os.makedirs(deployment_path)
    
    # TODO
    gen_obj.preprocess(input_data)

    # sync the input data
    self.__class__.sync_input_dirs(self.gateway_host, 
                                     sync_list + [(os.path.join(session_dir, deployment_dir),'*')], 
                                     self.local_prefix, remote_prefix)
    cmd_st = self.compose_exec_cmd(compute_node, input_data_fn_remote, conf_fn_remote, res_name, dep_config_name)
    print cmd_st
    subprocess.Popen(cmd_st,
                     shell=True, stdout=subprocess.PIPE,
                     stdin=subprocess.PIPE)
