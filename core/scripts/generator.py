
from abc import ABCMeta, abstractmethod, abstractproperty
import os
import time
import random
import math
import cStringIO
import imp

def get_gen_opts(gen_configs):
  generator_options = {}
  for k,v in gen_configs.items():
    if v.get('gen_path'):
      mod = imp.load_source(k, v['gen_path'])
      gen_class        = eval("mod.{gen_class}".format(**v))
    else:
      gen_class        = eval("{gen_class}".format(**v))

    generator_options[k] = gen_class()
  return generator_options 

class Generator(object):
  
  @abstractmethod
  def preprocessing(self, input_params):
    raise NotImplementedError( "Should have implemented this" )

  @abstractmethod
  def run(self, input_params):
    raise NotImplementedError( "Should have implemented this" )

  @abstractmethod
  def load(self, conn, d):
    raise NotImplementedError( "Should have implemented this" )
