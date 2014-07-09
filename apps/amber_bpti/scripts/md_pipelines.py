from abc import ABCMeta, abstractmethod, abstractproperty
import os
import cStringIO
import pipeline
import mddb_utils
import subprocess
import time
import sys
import chunkIt
import math
import numpy

try:
  import re
  import MDAnalysis
except:
  pass


class AmberPipeline(pipeline.Pipeline):
  __metaclass__ = ABCMeta

  def setup_params(self, d):
    self.param_dict = d
    self.inp_fn_list = ['leap.in', 'amber_min_1b', 'amber_min_2', 'amber_dyn_1', 'amber_dyn_2e', 'bpti_chk.rst']

    self.stage_list = [[{'cmd': 'tleap',
                         'args': '-f leap.in',
                         'i': 'leap.in',
                         'o': []}],
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
                       [{'cmd': 'cuda_spfp', 
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
                       [{'cmd': 'cuda_spfp',
                         'args': '-O -i amber_dyn_2e ' +\
                                    '-o amber_pmemd_cuda_dyn_2_tip4p_Ew.out ' +
                                    '-p protein.prmtop ' +\
                                    '-c protein_pmdmd_cuda_tip4p_Ew_dyn1_vel.rst ' +\
                                    '-r protein_pmdmd_cuda_tip4p_Ew_dyn2.rst ' +\
                                    '-x protein_pmemd_cuda_tip4p_Ew_dyn_2.nc ',
                         'i': 'amber_dyn_2e',
                         'o': ['amber_pmemd_cuda_dyn_2_tip4p_Ew.out']}]
                      ]        


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
    myu = MDAnalysis.Universe(dataDir+'bpti_from_mae.pdb',dataDir+'bpti-all/bpti-all-'+trj_id+'.dcd')
    myu.trajectory[t]
    s = myu.selectAtoms("not name pseu")
    cs2 = s.atoms.coordinates()

    t = t-1
    if t == -1:
      trj_id = "{0:03d}".format(int(trj_id) -1)
      myu = MDAnalysis.Universe(dataDir+'bpti_from_mae.pdb',dataDir+'bpti-all/bpti-all-'+trj_id+'.dcd')

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
  
#  @staticmethod
#  def output_vel(out_fn, myvel):
#    with open(out_fn, 'w') as ofp:
#      for i in range(0,len(myvel)-1,2):
#        ofp.write('{0:12.7f}'.format(myvel[i][0])+\
#                  '{0:12.7f}'.format(myvel[i][1])+\
#                  '{0:12.7f}'.format(myvel[i][2])+\
#                  '{0:12.7f}'.format(myvel[i+1][0])+\
#                  '{0:12.7f}'.format(myvel[i+1][1])+\
#                  '{0:12.7f}'.format(myvel[i+1][2])+'\n')
#  
#      ofp.write('{0:12.7f}'.format(myvel[-1][0])+\
#                '{0:12.7f}'.format(myvel[-1][1])+\
#                '{0:12.7f}'.format(myvel[-1][2])+'\n')
  

  @staticmethod
  def output_vel(out_fn, myvel):
    with open(out_fn, 'w') as ofp:
      #print myvel
      #print '=========================================='
      #print str(myvel)
      ofp.write(str(myvel))


  
  @staticmethod
  def preprocess(d, local_paths):
    trj_id      = d['trj_id']
    t           = d['t']

    output_prefix = d['output_prefix']
    if d['source'] == 'deshaw':
      dataDir = '/damsl/mddb/data/deshaw/MD0_DATA/Public/outgoing/science2010/DESRES-Trajectory-bpti-all/'
      if not os.path.exists(output_prefix):
        os.makedirs(output_prefix)

      vec_vel = AmberPipeline.calc_vel(dataDir, trj_id, t)
      #print "vec_vel", vec_vel
      AmberPipeline.output_vel(os.path.join(output_prefix, 'vel.txt'), vec_vel)

      sys.stdout.flush()
      sys.stderr.flush()
      myu = MDAnalysis.Universe(dataDir+'bpti_from_mae.pdb',dataDir+'bpti-all/bpti-all-'+trj_id+'.dcd')
      npu = myu.selectAtoms('not name pseu')
      if t:
          myu.trajectory[t]

      npu.write(os.path.join(output_prefix, 'temp'+trj_id+'.pdb'))

      pfile = open(os.path.join(output_prefix, 'temp'+trj_id+'.pdb'),'r')
      nfile = open(os.path.join(output_prefix,'protein.pdb'),'w')

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
      os.remove(os.path.join(output_prefix, 'temp'+trj_id+'.pdb'))
      nfile.close()


  def run_substage(self, output_prefix, substage):
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
