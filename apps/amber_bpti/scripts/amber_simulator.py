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

import re
try:
  import MDAnalysis
except:
  pass

class Amber_Simulator(pipeline_generator.Pipeline_generator):

    def set_params(self, run_dict):
      output_prefix = os.path.join(run_dict['resource_prefix'],
                                   run_dict['io_dir'],
                                   run_dict['session_dir'],
                                   run_dict['deployment_dir'],
                                   run_dict['run_dir'])
      run_dict['output_prefix'] = output_prefix
      self.template_fn_list = ['amber_dyn_2e']
      self.input_fn_list = ['leap.in', 'amber_min_1b', 'amber_min_2', 'amber_dyn_1', 'bpti_chk.rst']
      self.param_dict = run_dict
      self.stage_list = [[{'cmd': 'tleap',
                           'args': '-f leap.in',
                           'i': 'leap.in',
                           'o': []},
                          {'func': self.append_to_file,
                           'args':  ('protein.rst', 
                                     "51.2600000  51.2600000  51.2600000  90.0000000  90.0000000  90.0000000\n")
                          }
                         ],
                         [{'cmd': 'sander',
                           'args': '-O -i amber_min_1b ' +\
                                      '-o min1bout ' +\
                                      '-p protein.prmtop ' +\
                                      '-c protein.rst ' +\
                                      '-r protein_min1.rst ' +\
                                      '-ref protein.rst',
                           'i': 'amber_min_1b',
                           'o': ['min1bout']}],
                         [{'cmd': 'sander',
                           'args': '-O -i amber_min_2 ' +\
                                      '-o amber_min_2_tip4p_Ew.out ' +\
                                      '-p protein.prmtop ' +\
                                      '-c protein_min1.rst ' +\
                                      '-r protein_min2.rst',
                           'i': 'amber_min_2',
                           'o': ['amber_min_2_tip4p_Ew.out']}],
                         [{'cmd': 'pmemd.cuda_SPFP',
                           'args': '-O -i amber_dyn_1 ' +\
                                      '-o amber_pmemd_cuda_dyn_1_tip4p_Ew.out '+\
                                      '-p protein.prmtop '+\
                                      '-c protein_min2.rst ' +\
                                      '-r protein_pmdmd_cuda_tip4p_Ew_dyn1.rst ' +\
                                      '-x protein_pmemd_cuda_tip4p_Ew_dyn_1.mdcrd ' +\
                                      '-ref protein_min2.rst',
                           'i': 'amber_dyn_1',
                           'o': ['amber_pmemd_cuda_dyn_1_tip4p_Ew.out']}],
                         [{'func': self.reassign_vel,
                           'args': ('protein_pmdmd_cuda_tip4p_Ew_dyn1.rst', 'vel.txt', 'protein_pmdmd_cuda_tip4p_Ew_dyn1_vel.rst')}],
                         [{'cmd': 'pmemd.cuda_SPFP',
                           'args': '-O -i amber_dyn_2e ' +\
                                      '-o amber_pmemd_cuda_dyn_2_tip4p_Ew.out ' +
                                      '-p protein.prmtop ' +\
                                      '-c protein_pmdmd_cuda_tip4p_Ew_dyn1_vel.rst ' +\
                                      '-r protein_pmdmd_cuda_tip4p_Ew_dyn2.rst ' +\
                                      '-x protein_pmemd_cuda_tip4p_Ew_dyn_2.nc ',
                           'i': 'amber_dyn_2e',
                           'o': ['amber_pmemd_cuda_dyn_2_tip4p_Ew.out']}]
                        ]

    def append_to_file(self, output_prefix, fn, content):
      with open(os.path.join(output_prefix, fn), 'w') as ofp:
        ofp.write(content)

    def reassign_vel(self, output_prefix, rst1_fn, vel_fn, rst2_fn):
      with open(os.path.join(output_prefix, rst1_fn), 'r') as ifp:
        rst1 = ifp.readlines()
        vec_vel1 = []
        n = len(rst1)
        for l in rst1[n/2+1:n-1]:
          vec_vel1   = vec_vel1 + list(chunkIt.fixed_size(l.split(), 3))
        speeds1    = map(lambda v: math.sqrt(float(v[0])**2 + float(v[1])**2 + float(v[2])** 2), vec_vel1)

      with open(os.path.join(output_prefix, vel_fn), 'r') as ifp:
        vec_vel2 = eval(ifp.read())

        speeds2 = map(lambda v: math.sqrt(v[0]**2 + v[1]**2 + v[2] ** 2), vec_vel2)

      factor = numpy.mean(speeds1)/numpy.mean(speeds2)

      vec_vel2 = map(lambda v: map(lambda x: x*factor, v), vec_vel2)

      with open(os.path.join(output_prefix, rst2_fn), 'w') as ofp:
        n = len(rst1)
        ofp.write(''.join(rst1[0:n/2+1]))
        for i in range(0,len(vec_vel2)):
          ofp.write('{0:12.7f}{1:12.7f}{2:12.7f}'.format(*(map(float, vec_vel2[i]))))
          if i % 2 or i == len(vec_vel2) -1:
            ofp.write('\n')
        ofp.write(rst1[-1])
 

    @staticmethod 
    def calc_vel(dataDir, trj_id, t):
      pdb_file = os.path.join(dataDir, 'bpti_from_mae.pdb')
      dcd_file = os.path.join(dataDir, 'bpti-all', 'bpti-all-'+trj_id+'.dcd')
      myu = MDAnalysis.Universe(pdb_file, dcd_file)
      myu.trajectory[t]
      s = myu.selectAtoms("not name pseu")
      cs2 = s.atoms.coordinates()
  
      t = t-1
      if t == -1:
        trj_id = "{0:03d}".format(int(trj_id) -1)
        myu = MDAnalysis.Universe(pdb_file, dcd_file)
  
      myu.trajectory[t]
      s = myu.selectAtoms("not name pseu")
      cs1 = s.atoms.coordinates()
  
      vec_vel = []
      vec_pos = []
      for i in range(len(cs2)):
          vec_pos.append(cs2[i])
          vec_vel.append(cs2[i]-cs1[i]) #converting to amber unit
  
      vec_vel = map(lambda x: x.tolist(), vec_vel)
      return vec_vel

    @staticmethod
    def output_vel(out_fn, myvel):
      with open(out_fn, 'w') as ofp:
        ofp.write(str(myvel))   
   
    # write the input_data to local input_data_fn
    def preprocess(self, d):
      # TODO
      print "+++++ Amber preprocessing +++++"  
      trj_id = d['trj_id']
      t = d['t']

      deployment_path = d['deployment_path']
      # TODO
      print " +++++ Deployment path +++++"
      print deployment_path

      dataDir = d['data_source'] 

      if not os.path.exists(deployment_path):
        os.makedirs(deployment_path)

      vec_vel = Amber_Simulator.calc_vel(dataDir, trj_id, t)
      Amber_Simulator.output_vel(os.path.join(deployment_path, 'vel.txt'), vec_vel)

      sys.stdout.flush()
      sys.stderr.flush()
      
      pdb_file = os.path.join(dataDir, 'bpti_from_mae.pdb')
      dcd_file = os.path.join(dataDir, 'bpti-all', 'bpti-all-' + trj_id + '.dcd') 
      myu = MDAnalysis.Universe(pdb_file, dcd_file)
      print "+++++ GENERATED UNIVERSE OBJECT +++++"
      npu = myu.selectAtoms('not name pseu')
      if t:
          myu.trajectory[t]

      npu.write(os.path.join(deployment_path, 'temp'+trj_id+'.pdb'))

      pfile = open(os.path.join(deployment_path, 'temp'+trj_id+'.pdb'),'r')
      nfile = open(os.path.join(deployment_path,'protein.pdb'),'w')

      for line in pfile:
          tm = re.search(r' [0-9][A-Z][A-Z] ',line)
          if not tm:
              if 'CL' in line:
                  nfile.write(line.replace('CL ','Cl-',2))
              elif 'CYS' in line:
                  nfile.write(line.replace('CYS','CYX'))
              else:
                  nfile.write(line)
      pfile.close()
      os.remove(os.path.join(deployment_path, 'temp'+trj_id+'.pdb'))
      nfile.close()
      return

    def run_substage(self, output_prefix, substage):
      out_fns = substage.get('o')
      in_fn   = substage.get('i')
      func   = substage.get('func')
      args   = substage.get('args')
      if func != None:
        func(output_prefix,*args)
        return True
      else:
        cmd   = substage.get('cmd')
        cmd   = self.cmd_dict.get(cmd) or cmd
 
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
                               'PATH': os.getenv('PATH'),
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
      return

    def get_output_fns(self, d):
      output_dict = {}
      return output_dict

