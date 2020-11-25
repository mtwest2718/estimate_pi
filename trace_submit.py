import htcondor
import pdb

njobs = 32

input_files = [f"samples_{j}.csv" for j in range(njobs)] 

trace_sub = htcondor.Submit(
    executable = 'pi_trace.py',
    arguments = '--infiles ' + ' '.join(input_files),
    should_transfer_files = "YES",
    transfer_input_files = 'results/',
    log = 'log/trace.log',
    output = 'out/trace.out',
    error = 'err/trace.err',
    request_cpus = '1',
    request_memory = '3GB',
    request_disk = '500MB'
)

schedd = htcondor.Schedd()
with schedd.transaction() as txn:
    trace_sub.queue(txn)

