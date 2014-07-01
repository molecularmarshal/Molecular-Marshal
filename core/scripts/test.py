class TObj:
    
  def int2dirname(self, dir_prefix, i):
    s = '{0:06x}'.format(i)
    return dir_prefix + '-'.join(s[i:i+2] for i in range(0, len(s), 2))

  def get_deployment_name(self, deployment_id):
    return self.int2dirname('d_', deployment_id)

if __name__ == "__main__":
    t = TObj()
    print t.int2dirname('d_', 2)
