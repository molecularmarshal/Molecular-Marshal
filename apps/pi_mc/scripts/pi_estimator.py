from __future__ import division
import random
import generator

class PI_Estimator(generator.Generator):

    """
    input_params:
    {'x_seed': x_seed, 'y_seed':y_seed, 'num_samples':#}
    """
    def preprocessing(self, input_params):
        print "preprocessing is not required" 

    def run (self, input_params):
        num_samples = input_params['num_samples']
        rand_x = random.Random()
        rand_y = random.Random()
        rand_x.seed(input_params['x_rand_seed'])
        rand_y.seed(input_params['y_rand_seed'])
        num_inside = 0
        num_outside = 0

        for i in range(0,num_samples):
            tmp_x = rand_x.random()
            #print "x: " + str(tmp_x)

            tmp_y = rand_y.random()
            #print "y: " + str(tmp_y)

            tmp_result = (tmp_x*tmp_x) + (tmp_y*tmp_y)
            #print "result: " + str(tmp_result)

            if tmp_result > 1:
                num_outside = num_outside + 1
            else:
                num_inside = num_inside + 1

        pi = (num_inside/num_samples)*4
        return pi

    def load(self, conn, d):
      print d
      cur = conn.cursor()
      cur.execute(
                   """INSERT INTO pi_results (job_id, num_samples, pi_value)
                       VALUES (%s, %s, %s);""",
                   (d['jq_entry_id'], d['num_samples'], d['result']))
      conn.commit()
      cur.close()
      conn.close()
if __name__ == '__main__':
    mcPi = PI_Estimator()
    inp_params = {
            'x_seed': random.random(),
            'y_seed': random.random(),
            'num_samples': 100000}
    result = mcPi.run(inp_params)
    #mcPi.parse_gen_opts()
    print result
