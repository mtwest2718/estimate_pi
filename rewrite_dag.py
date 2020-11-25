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
