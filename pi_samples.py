#!/usr/bin/env python3
import argparse
import sys
from scipy.stats import uniform
import pandas as pd
import numpy as np
from numpy.random import choice, default_rng
from multiprocessing import Pool, cpu_count
import pdb

def sample_square(seed, iters):
    samples = pd.DataFrame(
        uniform.rvs(size=[iters,2], random_state=seed), columns=['x','y']
    )
    # Estimate PI using area under a curve
    samples['in_circle'] = samples.x**2+samples.y**2 < 1
    samples['cumm_success'] = samples.in_circle.cumsum()
    samples['est_area'] = 4*samples.cumm_success/np.arange(1,iters+1)
    
    # Estimate PI using stochastic integration of (1-x^2)
    Z_x = (1-samples.x**2)**0.5
    samples['est_func'] = 4*Z_x.cumsum()/np.arange(1,iters+1)

    return samples

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--seed', type=int, default=42,
        help="Random seed")
    parser.add_argument('-i', '--iters', type=int, default=1000,
        help="Number of iterations")
    parser.add_argument('-t', '--threads', type=int, default=1,
        help="Number of multiprocessing threads")
    parser.add_argument('-o', '--outfile', default='samples.csv',
        help="Output filename")
    args = parser.parse_args()

    if args.iters % args.threads != 0:
        sys.exit("--iters ({}) must be a multiple of --threads ({})".format(args.iters, args.threads))

    # Generate samples in unit square and check if within quarter circle
    if args.threads > 1:
        rng = default_rng(args.seed)
        seeds = rng.choice(1000000, args.threads, replace=False)
        # Define input arguments for each sampling subprocess func call
        sample_args = tuple(
            zip(seeds, np.repeat(args.iters // args.threads, args.threads))
        )
        with Pool(processes=args.threads) as pool:
            samples = pool.starmap(sample_square, sample_args)
            for k, df in enumerate(samples):
                # Add thread number to output samples filename
                outfile = args.outfile.replace('.csv', '_{}.csv'.format(k))
                df.to_csv(outfile, index=False)
    else:
        samples = sample_square(args.seed, args.iters)
        samples.to_csv(args.outfile, index=False)
