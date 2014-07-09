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

    # write the input_data to local input_data_fn
    def preprocess(self, input_params):
      return


    def run_substage(self, substage):
      return True


    def load(self, conn, result_dir, d, local_paths):
      return


 
    def get_output_fns(self, d):
      output_dict = {}
      return output_dict

