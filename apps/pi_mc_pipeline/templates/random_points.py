import sys
import json
import random
import os

def gen_rand_points(input_fn):

    with open(input_fn, 'r') as ifp:
        input_params = eval(ifp.read())
    num_samples = input_params['num_samples']
    rand_x = random.Random()
    rand_y = random.Random()
    rand_x.seed(input_params['x_rand_seed'])
    rand_y.seed(input_params['y_rand_seed'])
    
    with open('points.json', 'wb') as ifp:
        for i in range(0, num_samples):
            tmp_x = rand_x.random()
            tmp_y = rand_y.random()
            tmp_dict = {}
            tmp_dict['x'] = tmp_x
            tmp_dict['y'] = tmp_y
            tmp_dict['num_samples'] = num_samples
            ifp.write(json.dumps(tmp_dict))
            ifp.write('\n')
    print "Stage 1 done"
    os.remove(input_fn) 
    print "clean up the working dir"

if __name__ == '__main__':
    gen_rand_points(sys.argv[1])

