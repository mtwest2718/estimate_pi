#!/usr/bin/env python3
import argparse
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import pdb
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--infiles', nargs='+', help="list of input files")
    parser.add_argument('--estimator', dest='est', choices=['area', 'func'],
        help="Which estimator type to plot")
    args = parser.parse_args()

    print("Generating base figure")
    fig, ax = plt.subplots()
    ax.set(xlabel='Samples (#)', ylabel='Estimate')
    ax.set(title=r'Stochastic estimate of $\pi$ using {}'.format(args.est.upper()))
    ax.grid()

    col = 'est_' + args.est
    for m, csvfile in enumerate(args.infiles):
        df = pd.read_csv(csvfile, usecols=[col])
        df.rename(columns={col: 'estimate'}, inplace=True)
        # subset of estimates where #-samples is an power of 2
        sample_idx = 2**np.arange(0, np.floor(np.log2(len(df)))+1)
        subset_df = df.iloc[sample_idx-1]

        # plot trace with semilog-X axis in base 10
        print("\tPlot data from {}".format(csvfile))
        ax.semilogx(sample_idx, subset_df.estimate, '.-', c='blue', alpha=0.5)

    ax.set_yticks(np.arange(0,1.5,0.25)*np.pi)
    ax.set_ylim(-0.1, 4.1)
    ax.set_yticklabels(
        ['0', r'$\pi/4$', r'$\pi/2$', r'$3\pi/4$', r'$\pi$', r'$5\pi/4$']
    )

    print("Saving figure to disk")
    fig.savefig('pi_trace_{}.png'.format(args.est))
