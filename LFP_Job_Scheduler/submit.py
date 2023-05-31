#Submit a job
import utility
import sys
sys.path.append('/hlabhome/wg-mjames')
sys.path.append('/hlabhome/wg-mjames/.conda/envs/LFP_job')

print(utility.safe_submit_job())