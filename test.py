CUR_DIR = '/hlabhome/wg-mjames/Job_Scheduler/'
with open(CUR_DIR+"test.log", 'a') as f:
    f.write('job triggered')

from LFP_Job_Scheduler import utility

with open(CUR_DIR+"test.log", 'a') as f:
    f.write('job finished\n')