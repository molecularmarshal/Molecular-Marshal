import resources
import os

class DummyResource(resources.RemoteResource):
  gateway_host = "dummy" 
  resource_prefix = "/home/dummy/bigdigsci_data" 
  
  @staticmethod
  def get_paths():
    path_dict = \
      {
       'io_dir':          resources.Resource.io_dir,
       'resource_prefix': DummyResource.resource_prefix,
      }
    return path_dict

  @staticmethod
  def get_environ():
    d = { "PYTHONPATH": [os.path.join(DummyResource.resource_prefix,
                                      'core/scripts')]}
    return d
