#!/usr/bin/env python3

import argparse
from random import sample, seed
from pathlib import Path
from os import mkdir
from os.path import isdir, join
import pdb
# HTCondor libraries
import htcondor
from htcondor import dags

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--seed', type=int, default=42,
        help="Root RNG seed")
    parser.add_argument('-j', '--njobs', type=int, default=1,
        help="Number of parallel jobs")
    parser.add_argument('-i', '--iters', type=int, default=1000,
        help="Number of iterations per job")
    parser.add_argument('-t', '--threads', type=int, default=1,
        help="Number of multiprocessing threads")
    parser.add_argument('--submit', action='store_true', default=False,
        help="Generate DAG *AND* submit workflow to queue")
    args = parser.parse_args()

    if args.iters % args.threads != 0:
        sys.exit("--iters ({}) must be a multiple of --threads ({})".format(args.iters, args.threads))

    ## Set up directory structure for job log, out and error
    this_dir = Path(__file__).parent

    ## Making the DAG
    pi_dag = dags.DAG()

    sample_sub = htcondor.Submit(
        executable = 'pi_samples.py',
        arguments = '--seed $(seed) --iters $(iters) --threads $(threads) --outfile $(outfile)',
        should_transfer_files = "YES",
        transfer_output_files = "$(outfile)",
        initialdir = 'results',
        log = '../log/samples.log',
        output = '../log/samples.out',
        error = '../log/samples.err',
        request_cpus = '$(threads)',
        request_memory = '1GB',
        request_disk = '500MB',
    )
    # root RNG seed
    seed(args.seed)
    seed_nums = sample(range(1000000), k=args.njobs)
    # construct input arg dicts for sampling jobs
    sample_vars = []
    for i in range(args.njobs):
        sample_vars.append(
            {'seed': str(seed_nums[i]), 'iters': str(args.iters), 'threads': str(args.threads),
            'outfile': 'samples_{}.csv'.format(i)}
        )
    # Add sampling jobs layer to DAG
    sample_layer = pi_dag.layer(
        name = 'sample', submit_description = sample_sub,
        vars = sample_vars
    )

    # Define the Trace plot Jobs (submit file)
    files_list = ' '.join(f"samples_{j}.csv" for j in range(args.njobs))
    trace_sub = htcondor.Submit(
        executable = 'pi_trace.py',
        arguments = '--infiles $(infiles) --estimator $(est_type)',
        should_transfer_files = "YES",
        transfer_input_files = 'results/',
        log = 'log/trace.log',
        output = 'log/trace.out',
        error = 'log/trace.err',
        request_cpus = '1',
        request_memory = '1GB',
        request_disk = '3GB',
    )
    # construct input arg dicts for trace plotting jobs
    trace_vars = []
    for est_type in ['area']:
        trace_vars.append({'infiles': files_list, 'est_type': est_type})
    # add the summary job layer to DAG
    trace_layer = sample_layer.child_layer(
        name = 'trace', submit_description = trace_sub,
        vars = trace_vars
    )

    # Write DAG file to disk
    dag_file = dags.write_dag(pi_dag, this_dir, dag_file_name='pi.dag')

    if args.submit:
        # Generate condor_submit file for DAG
        dag_submit = htcondor.Submit.from_dag(
            str(dag_file),
            {'force': 1, 'batch-name': 'MmmmmPi'}
        )

        schedd = htcondor.Schedd()
        # Connect to the Scheduler and submit the DAGman job
        with schedd.transaction() as txn:
            cluster_id = dag_submit.queue(txn)
