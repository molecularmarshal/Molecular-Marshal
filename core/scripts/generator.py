
from abc import ABCMeta, abstractmethod, abstractproperty
import os
import time
import random
import math
import cStringIO


class Generator(object):
  
  generator_options = {}
 
  @staticmethod
  def parse_gen_opts(gen_configs):
    for k,v in gen_configs.items():
      if v.get('gen_path'):
        mod = imp.load_source(k, v['gen_path'])
        gen_class        = eval("mod.{gen_class}".format(**v))
      else:
        gen_class        = eval("{gen_class}".format(**v))
  
      Generator.generator_options[k] = gen_class()
    return
  

  @abstractmethod
  def preprocessing(self, input_params):
    raise NotImplementedError( "Should have implemented this" )

  @abstractmethod
  def run(self, input_params):
    raise NotImplementedError( "Should have implemented this" )

  @abstractmethod
  def load(self, conn, d):
    raise NotImplementedError( "Should have implemented this" )
