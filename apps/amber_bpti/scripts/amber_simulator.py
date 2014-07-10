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
import MDAnalysis

class Amber_Simulator(pipeline_generator.Pipeline_generator):

    def set_params(self, run_dict):
      output_prefix = os.path.join(run_dict['resource_prefix'],
                                   run_dict['io_dir'],
                                   run_dict['session_dir'],
                                   run_dict['deployment_dir'],
                                   run_dict['run_dir'])
      run_dict['output_prefix'] = output_prefix
      self.param_dict = run_dict
      self.stage_list = []

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
      dcd_file = os.path.join(dataDir, 'bpti-all', 'bpti-all-001.dcd') 
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

    def run_substage(self, substage):
      return True

    def load(self, conn, result_dir, d, local_paths):
      return

    def get_output_fns(self, d):
      output_dict = {}
      return output_dict

