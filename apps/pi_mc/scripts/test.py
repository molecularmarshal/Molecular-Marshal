import generator

if __name__ == '__main__':

  with open('gen_config.txt', 'r') as ifp:
    d = eval(ifp.read())

  generator.Generator.parse_gen_opts(d['generators'])
  print generator.Generator.generator_options  
