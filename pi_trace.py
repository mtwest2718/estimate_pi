#!/usr/bin/env python3
import argparse
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import pdb
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--infile', help="summary of estimates")
    args = parser.parse_args()

    print("Generating base figure")
    fig, ax = plt.subplots()
    ax.set(xlabel='Samples (#)', ylabel='Estimate')
    ax.set(title=r'Stochastic estimate of $\pi$ using AREA')
    ax.grid()

    df = pd.read_csv(args.infile)
    idx = np.array(df.columns, dtype=np.uint)

    # plot trace with semilog-X axis in base 10
    print("\tPlot data from {}".format(args.infile))
    for n in range(len(df)):
        ax.semilogx(idx, df.iloc[n], '.-', c='blue', alpha=0.5)

    ax.set_yticks(np.arange(0,1.5,0.25)*np.pi)
    ax.set_ylim(-0.1, 4.1)
    ax.set_yticklabels(
        ['0', r'$\pi/4$', r'$\pi/2$', r'$3\pi/4$', r'$\pi$', r'$5\pi/4$']
    )

    print("Saving figure to disk")
    fig.savefig('pi_trace.png')
