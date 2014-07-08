def caculate_pi(self, output_prefix, iter_fn):
    num_inside = 0
    num_outside = 0

    with open (iter_fn, 'r') as ifp:
        oup = ifp.readlines()

    for line in oup:
        tmp_dict = jump.loads(line)
        tmp_x = tmp_dict['x']
        tmp_y = tmp_dict['y']
        if (tmp_x**2 + tmp_y**2) > 1:
            num_outside = num_outside + 1
        else:
            num_inside = num_inside + 1

    pi = (num_inside/num_samples)*4

    with open(os.path.join(output_prefix, input_params['result_fn']), 'w') as ofp:
        ofp.write('{0}\t{1}\t{2}'.format(input_params['jq_entry_id'], 
                                         input_params['num_samples'], pi))
if __name__ == '__main__':
    output_prefix = sys.argv[1]
    caculate_pi(self, output_prefix, 'points.json'):

