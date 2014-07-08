import os
import subprocess

class test_pipeline:

    def __init__(self):

        with open('job', 'r') as ifp:
            tmp_dict = eval(ifp.read())
        
        self.tmp_dict = tmp_dict
        self.output_prefix = os.getcwd()
        
        self.stage_list = [[{"cmd": "python",
                             "i": "random_points.py",
                             "a": self.tmp_dict,
                             "o": "points.json"
                             }],
                           [{"cmd": "python",
                             "i": "count_points.py",
                             "a": "points.json"
                             }]
                          ]
    
    def run_substage(self):
        # stage1
        test_stage = self.stage_list[0][0]
        cmd = test_stage['cmd']
        in_fn = test_stage['i']
        args = test_stage['a']
        tmp_file = 'tmp_file1'
        with open(tmp_file, 'w') as ifp:
            ifp.write(str(args))
        subprocess.call(cmd + ' ' + in_fn + ' ' + tmp_file, shell=True, 
                        cwd = self.output_prefix, stderr = subprocess.STDOUT,
                        stdout = None)
        
        # stage2
        test_stage_2 = self.stage_list[1][0]
        cmd = test_stage_2['cmd']
        in_fn = test_stage_2['i']
        args = test_stage_2['a']
        subprocess.call(cmd + ' ' + in_fn + ' ' + self.output_prefix + ' ' + args, 
                        shell=True, cwd = self.output_prefix, 
                        stderr = subprocess.STDOUT, stdout = None)

if __name__ == '__main__':
    tp = test_pipeline()
    tp.run_substage()
     
