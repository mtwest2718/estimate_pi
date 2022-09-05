import htcondor
import pdb

sample_files = ['samples_0.csv', 'samples_1.csv', 'samples_2.csv', 'samples_3.csv', 'samples_4.csv', 'samples_5.csv', 'samples_6.csv', 'samples_7.csv', 'samples_8.csv', 'samples_9.csv']
outfile = 'summ_estimate.csv'

summ_sub = htcondor.Submit(
    executable = 'pi_summary.py',
    arguments = '--infiles $(infiles) --outfile $(outfile)',
    should_transfer_files = "YES",
    initialdir = 'results',
    transfer_input_files = '$(infiles_comma)',
    log = '../logs/summ.log',
    output = '../logs/summ.out',
    error = '../logs/summ.err',
    request_cpus = '1',
    request_memory = '1GB',
    request_disk = '1GB',
)
summ_vars = [{'infiles': 'samples*.csv', 'outfile': outfile, 'infiles_comma': ','.join(sample_files)}]

pdb.set_trace()

schedd = htcondor.Schedd()
submit_result = schedd.submit(summ_sub, itemdata=iter(summ_vars))
