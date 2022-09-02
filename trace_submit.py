import htcondor
import pdb

summ_file = 'summ_estimate.csv'

# Define the Trace plot Jobs (submit file)
trace_sub = htcondor.Submit(
    executable = 'pi_trace.py',
    arguments = '--infile $(infile)',
    should_transfer_files = "YES",
    initialdir = 'results/',
    transfer_input_files = '$(infile)',
    log = '../logs/trace.log',
    output = '../logs/trace.out',
    error = '../logs/trace.err',
    request_cpus = '1',
    request_memory = '1GB',
    request_disk = '1GB',
)
# construct input arg dicts for trace plotting jobs
trace_vars = [{'infile': summ_file}]

schedd = htcondor.Schedd()
submit_result = schedd.submit(trace_sub, itemdata=iter(trace_vars))
