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
        arguments = '--seed $(seed) --iters $(iters) --threads $(threads) --outfile samples_$(ProcId).csv',
        should_transfer_files = "YES",
        initialdir = 'results',
        log = '../log/samples.log',
        output = '../out/samples_$(ProcId).out',
        error = '../err/samples_$(ProcId).err',
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
        sample_vars.append({'seed': str(i), 'iters': str(iters), 'threads': str(threads)}
    # Add sampling jobs layer to DAG
    sample_layer = pi_dag.layer(
        name = 'sample', submit_description = sample_sub,
        vars = sample_vars
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
