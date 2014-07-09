from __future__ import division
import random
import generator
import pipeline_generator
import os
import os.path
import subprocess
import json
import sys
import time

class PI_Estimator_Pipeline(pipeline_generator.Pipeline_generator):

    """
    input_params:
    {'x_seed': x_seed, 'y_seed':y_seed, 'num_samples':#}
    """
    
    def set_params(self, run_dict):
      output_prefix = os.path.join(run_dict['resource_prefix'], 
                                   run_dict['io_dir'], 
                                   run_dict['session_dir'], 
                                   run_dict['deployment_dir'],
                                   run_dict['run_dir'])
      run_dict['output_prefix'] = output_prefix
      self.param_dict = run_dict
      self.stage_list = [[{"cmd": "python",
                           "i": "random_points.py",
                           "a": self.param_dict,
                           "t_f": "tmp_file1",
                           "o": "points.json"
                          }],
                         [{"cmd": "python",
                           "i": "count_points.py",
                           "a": "points.json",
                           "o": "result.txt"
                          }]
                        ]
    
    # write the input_data to local input_data_fn
    def preprocess(self, input_params):
      '''  
      deployment_path = input_params[0]['deployment_path']
      if not os.path.exists(deployment_path):
          os.mkdirs(path)
      filename = input_params[0]['fn_name']

      with open(os.path.join(deployment_path, filename), 'w') as ifp:
          ifp.write(json.dumps(input_params))
      '''
      return
    
    # customized run_substage method 
    def run_substage(self, substage):

      output_prefix = self.param_dict['output_prefix']

      out_fns = substage.get('o')
      # input file in template_dir
      in_fn = os.path.join(self.param_dict['resource_prefix'],
                           self.param_dict['app_dir'],
                           self.param_dict['template_dir'],
                           substage.get('i'))

      cmd = substage.get('cmd')
      args = substage.get('a')

      tmp_file = substage.get('t_f')
      if tmp_file:
          # tmp_file in output_prefix dir
          with open(os.path.join(output_prefix, tmp_file), 'w') as ifp:
              ifp.write(str(args))
       
      print '==============================================================='
      print 'in_fn:', in_fn
      print 'output_prefix:', output_prefix
      print 'command:\n', cmd, "", in_fn, "", (tmp_file if tmp_file else args)
      print 'out_fns: ', ', '.join(out_fns)
      print '==============================================================='
  
      sys.stdout.flush()
  
      log_fn = substage.get('i') + '.log'
      with open(os.path.join(output_prefix, log_fn), "w") as debug_log:
        subprocess.call(cmd + ' ' + in_fn + ' ' + (tmp_file if tmp_file else args), 
                        shell=True, cwd = output_prefix,
                        stderr=subprocess.STDOUT, stdout=debug_log)
      time.sleep(0.1)
  
      sys.stdout.flush()
      return True

    def load(self, conn, result_dir, d, local_paths):
      print d
      cur = conn.cursor()

      with open(os.path.join(result_dir, d['result_fn'])) as ifp:
        cur.copy_from(ifp, 'pi_results', 
                      columns=('job_id', 'num_samples', 'pi_value'))

      conn.commit()
      cur.close()

    def get_output_fns(self, d):
      output_dict = {'result_fn': 'result.txt'}
      return output_dict

