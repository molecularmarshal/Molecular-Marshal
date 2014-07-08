from __future__ import division
import os
import sys
import json

def caculate_pi (output_prefix, iter_fn):
    num_inside = 0
    num_outside = 0

    with open (iter_fn, 'r') as ifp:
        oup = ifp.readlines()

    for line in oup:
        tmp_dict = json.loads(line)
        tmp_x = tmp_dict['x']
        tmp_y = tmp_dict['y']
        num_samples = tmp_dict['num_samples']
        if (tmp_x**2 + tmp_y**2) > 1:
            num_outside = num_outside + 1
        else:
            num_inside = num_inside + 1
    pi = (num_inside/num_samples)*4
    
    #with open(os.path.join(output_prefix, input_params['result_fn']), 'w') as ofp:
    #    ofp.write('{0}\t{1}\t{2}'.format(input_params['jq_entry_id'], 
    #                                     input_params['num_samples'], pi))
    
    with open(os.path.join(output_prefix, 'result.txt'), 'w') as ofp:
        ofp.write(str(pi))
    print "Stage 2 done"
    os.remove(iter_fn)
    print "clean up the dir"

if __name__ == '__main__':
    output_prefix = os.getcwd()
    caculate_pi(output_prefix, 'points.json')
