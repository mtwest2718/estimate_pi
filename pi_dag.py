#!/usr/bin/env python3
import argparse
import random
from pathlib import Path
from os import mkdir
from os.path import isdir, join
import pdb
# HTCondor libraries
import htcondor
from htcondor import dags

def cli_parser():
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
    parser.add_argument('-v','--verbose', action='store_true', default=False,
        help="Print out DAG creation steps")
    args = parser.parse_args()

    if args.iters % args.threads != 0:
        sys.exit("--iters ({}) must be a multiple of --threads ({})".format(args.iters, args.threads))

    return args


def sampling_jobs(rng_seed, njobs, iters, threads):
    # Define the Sampling Jobs (submit file)
    sample_sub = htcondor.Submit(
        executable = 'pi_samples.py',
        arguments = '--seed $(seed) --iters $(iters) --threads $(threads) --outfile $(outfile)',
        should_transfer_files = "YES",
        initialdir = 'results',
        log = '../logs/samples.log',
        output = '../logs/samples_$(id).out',
        error = '../logs/samples_$(id).err',
        request_cpus = '$(threads)',
        request_memory = '1GB',
        request_disk = '1GB',
    )
    # root RNG seed
    random.seed(rng_seed)
    seed_nums = random.sample(range(1000000), k=njobs)
    # construct input arg dicts for sampling jobs
    sample_vars = []
    for i in range(njobs):
        sample_vars.append(
            {'seed': str(seed_nums[i]), 'iters': str(iters), 'threads': str(threads),
            'outfile': f'samples_{i}.csv', "id": str(i)}
        )
    return {'submit': sample_sub, 'vars': sample_vars}

def summary_job(sample_files, outfile):
    summ_sub = htcondor.Submit(
        executable = 'pi_summary.py',
        arguments = '--infiles $(infiles) --outfile ${outfile}',
        should_transfer_files = "YES",
        transfer_input_files = 'results/',
        log = 'logs/summ.log',
        output = 'logs/summ.out',
        error = 'logs/summ.err',
        request_cpus = '1',
        request_memory = '1GB',
        request_disk = '1GB',
    )
    summ_vars = [{'infiles': ' '.join(sample_files), 'outfile': outfile}]
    return {'submit': summ_sub, 'vars': summ_vars}

def trace_plot_job(summ_file):
    # Define the Trace plot Jobs (submit file)
    trace_sub = htcondor.Submit(
        executable = 'pi_trace.py',
        arguments = '--infile $(infile)',
        should_transfer_files = "YES",
        transfer_input_files = 'results/',
        log = 'logs/trace.log',
        output = 'logs/trace.out',
        error = 'logs/trace.err',
        request_cpus = '1',
        request_memory = '1GB',
        request_disk = '1GB',
    )
    # construct input arg dicts for trace plotting jobs
    trace_vars = [{'infiles': summ_file}]
    return {'submit': trace_sub, 'vars': trace_vars}

def var_plot_job(summ_file):
    # Define the Variance plot Jobs (submit file)
    var_sub = htcondor.Submit(
        executable = 'pi_variance.py',
        arguments = '--infile $(infile)',
        should_transfer_files = "YES",
        transfer_input_files = 'results/',
        log = 'logs/var.log',
        output = 'logs/var.out',
        error = 'logs/var.err',
        request_cpus = '1',
        request_memory = '1GB',
        request_disk = '1GB',
    )
    # construct input arg dicts for trace plotting jobs
    var_vars = [{'infiles': summ_file}]
    return {'submit': var_sub, 'vars': var_vars}


if __name__ == "__main__":
    # parser the input arguments for DAG
    args = cli_parser()

    ## Set up directory structure for job log, out and error
    this_dir = Path(__file__).parent
    for dir in ['logs', 'results']:
        new_dir = join(this_dir, dir)
        if not isdir(new_dir):
            mkdir(new_dir)

    ## Making the DAG
    if args.verbose: print("Generate DAG object")
    pi_dag = dags.DAG()

    # Add sampling jobs layer to DAG
    sampling = sampling_jobs(args.seed, args.njobs, args.iters, args.threads)
    if args.verbose: print("\tAdd sampling layer to DAG")
    sample_layer = pi_dag.layer(
        name='sample',
        submit_description=sampling['submit'], vars=sampling['vars']
    )

    samp_files = [f['outfile'] for f in sampling['var']]
    if args.threads > 1:
        infiles = [F.replace('.csv', f"_{i}.csv") for i in range(threads) for F in samp_files]
    else:
        infiles = samp_files
    summ_file = 'summ_estimate.csv'
    # Add summary job layer to DAG
    if args.verbose: print("\tAdd summary layer to DAG")
    summary = summary_job(sfiles, summ_file)
    summ_layer = sample_layer.child_layer(
        name='summary',
        submit_description=summary['submit'], vars=summary['vars']
    )

    # Add the trace plotting job layer to DAG
    trace = trace_plot_job(summ_file)
    if args.verbose: print("\tAdd plotting traces layer to DAG")
    trace_layer = summary_layer.child_layer(
        name='trace',
        submit_description=trace['submit'], vars=trace['vars']
    )

    # Add the variance plotting job layer to DAG
    variance = var_plot_job(summ_file)
    if args.verbose: print("\tAdd plotting variance layer to DAG")
    var_layer = summary_layer.child_layer(
        name='variance',
        submit_description=variance['submit'], vars=variance['vars']
    )

    ## Write DAG file to disk
    if args.verbose: print("\tWrite out DAG and associated submit files")
    dag_file = dags.write_dag(pi_dag, this_dir, dag_file_name='pi.dag')

    ## Programmatically submit full workflow
    if args.submit:
        # Generate condor_submit file for DAG
        if args.verbose: print("\tWrite out submit file for DAG")
        dag_submit = htcondor.Submit.from_dag(
            str(dag_file), {'force': 1, 'batch-name': 'MmmmmPi'}
        )

        if args.verbose: print("Submit workflow to queue")
        # Connect to the Scheduler and submit the DAGman job
        schedd = htcondor.Schedd()
        schedd.submit(dag_submit)
