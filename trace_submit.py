import htcondor
import pdb

njobs = 4

input_files = ' '.join([f"samples_{j}.csv" for j in range(njobs)])
trace_vars = []

trace_sub = htcondor.Submit(
    executable = 'pi_trace.py',
    arguments = '--infiles $(infiles) --estimator $(est_type)',
    should_transfer_files = "YES",
    transfer_input_files = 'results/',
    log = 'log/trace.log',
    output = 'out/trace_$(ProcID).out',
    error = 'err/trace_$(ProcID).err',
    request_cpus = '1',
    request_memory = '3GB',
    request_disk = '500MB'
)

for est_type in ['area', 'func']:
    trace_vars.append({'infiles': input_files, 'est_type': est_type})

schedd = htcondor.Schedd()
with schedd.transaction() as txn:
    trace_sub.queue_with_itemdata(txn, itemdata=iter(trace_vars))
