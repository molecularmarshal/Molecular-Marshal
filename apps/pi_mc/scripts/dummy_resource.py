import resources

class DummyResource(resources.RemoteResource):

  res_prefix = None
  
  @staticmethod
  def get_path():
    path_dict = \
      {
       'io_dir':          resources.Resource.io_dir,
       'resource_prefix': DummyResource.res_prefix,
      }
    return path_dict

  @staticmethod
  def get_environ():
    d = { "PYTHONPATH": [os.path.join(DummyResource.res_prefix,
                                      'core/scripts')]}
    return d
