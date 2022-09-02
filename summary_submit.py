import htcondor
import pdb

sample_files = ['samples_0.csv']
outfile = 'summ_estimate.csv'

summ_sub = htcondor.Submit(
    executable = 'pi_summary.py',
    arguments = '--infiles $(infiles) --outfile $(outfile)',
    should_transfer_files = "YES",
    initialdir = 'results',
    transfer_input_files = '$(infiles)',
    log = '../logs/summ.log',
    output = '../logs/summ.out',
    error = '../logs/summ.err',
    request_cpus = '1',
    request_memory = '1GB',
    request_disk = '1GB',
)
summ_vars = [{'infiles': ' '.join(sample_files), 'outfile': outfile}]

schedd = htcondor.Schedd()
submit_result = schedd.submit(summ_sub, itemdata=iter(summ_vars))
