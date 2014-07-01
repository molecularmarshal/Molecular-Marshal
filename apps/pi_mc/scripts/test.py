import os

def read_configs():
  with open('dep_configs.txt', 'r') as ifp:
    dep_configs = eval(ifp.read())
  
  default_config = dep_configs['default']
  custom_configs = dep_configs['customized']
  
  configs = dict(map(lambda (k,v): (k,dict(default_config.items() + v.items())), 
                 custom_configs.items()))
  
  configs['default'] = default_config
  
  print configs
  
class c1:
  def __init__(self, d):
    map(lambda (k,v): setattr(self, k, v), d.items())

if __name__ == '__main__':
  config_name = 'gpu'
  configs = read_configs()
  obj = c1(configs[config_name])






