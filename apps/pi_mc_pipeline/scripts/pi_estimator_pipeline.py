from __future__ import division
import random
import generator
import pipeline_generator
import os
import os.path
import subprocess

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
      #TODO gen the tmp_dict as arg for python script
      tmp_dict =  
      self.stage_list = [[{"cmd": "python",
                           "i": "random_points.py",
                           "a": tmp_dict
                          }],
                         [{"cmd": "python",
                           "i": "count_points.py",
                           "a": output_prefix
                          }]
                        ]

    def preprocess(self, input_params):
      try:
          inpudt_fn = input_params[0]['input_fn']
      except Exception error:
          print "Error: " + str(error)
      with open(input_fn, 'w') as ifp:
          ifp.write(input_params)
      return
    
    def run_substage(self, param_dict):
      out_fns = substage.get('o')
      in_fn   = substage.get('i')

      func   = substage.get('func')
      if func != None:
        args   = substage.get('args')
        func(output_prefix,*args)
        return True
      else:
        cmd   = self.param_dict.get(substage.get('cmd'))
        args  = substage.get('args').format(**self.param_dict)
  
        print '==============================================================='
        print 'in_fn:', in_fn
        print 'output_prefix:', output_prefix
        print 'command:\n', cmd, " ", args
        print 'out_fns: ', ', '.join(out_fns)
        print '==============================================================='
  
        sys.stdout.flush()
  
        log_fn = (in_fn or 'temp') + '.log'
        with open(os.path.join(output_prefix, log_fn), "w") as debug_log:
  
          subprocess.call(cmd + ' ' + args, shell=True, cwd = output_prefix,
                          env={'AMBERHOME':os.getenv('AMBERHOME'),
                               'LD_LIBRARY_PATH': os.getenv('LD_LIBRARY_PATH') or "/user/lib",
                               'CUDA_VISIBLE_DEVICES': str(self.param_dict.get('device_id'))
                              },
                          stderr=subprocess.STDOUT, stdout=debug_log)
        time.sleep(0.1)
  
        out_fns.append(log_fn)
        for ofn in out_fns:
          print ofn, '------------------------------------------------------'
          subprocess.call("tail -n 40 {0}".format(ofn), shell=True, cwd = output_prefix,
                           stderr=subprocess.STDOUT)
  
  
        sys.stdout.flush()
        #return all(map(lambda out_fn: os.path.isfile(os.path.join(output_prefix, out_fn)), out_fns))
        return True

    def load(self, conn, result_dir, d, local_paths):
      print d
      cur = conn.cursor()

      with open(os.path.join(result_dir, d['result_fn'])) as ifp:
        cur.copy_from(ifp, 'pi_results', columns=('job_id', 'num_samples', 'pi_value'))

      conn.commit()
      cur.close()

    def get_output_fns(self, d):
      output_dict = {'result_fn': 'result.txt'}
      return output_dict

