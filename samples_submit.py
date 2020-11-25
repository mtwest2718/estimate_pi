from random import sample, seed
import htcondor

iters = 10000
njobs = 4
threads = 1
seed_num = 1984

# Define the Sampling Jobs (submit file)
sample_sub = htcondor.Submit(
    executable = 'pi_samples.py',
    arguments = '--seed $(seed) --iters $(iters) --threads $(threads) --outfile samples_$(ProcID).csv',
    should_transfer_files = "YES",
    initialdir = 'results',
    log = '../log/samples.log',
    output = '../out/samples_$(ProcID).out',
    error = '../err/samples_$(ProcID).err',
    request_cpus = '$(threads)',
    request_memory = '1GB',
    request_disk = '500MB',
)
# root RNG seed
seed(seed_num)
seed_nums = sample(range(1000000), k=njobs)
# construct input arg dicts for sampling jobs
sample_vars = []
for i in seed_nums:
    sample_vars.append({'seed': str(i), 'iters': str(iters), 'threads': str(threads)})

schedd = htcondor.Schedd()
with schedd.transaction() as txn:
    sample_sub.queue_with_itemdata(txn, itemdata=iter(sample_vars))
