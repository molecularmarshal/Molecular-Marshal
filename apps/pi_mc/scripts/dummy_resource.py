import resources
import os

class DummyResource(resources.RemoteResource):
  
  #TODO parse the config and dep_config and gen the dict
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
