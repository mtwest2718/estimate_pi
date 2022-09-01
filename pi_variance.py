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
    ax.set(xlabel='Samples (#)', ylabel='VAR')
    ax.set(title=r'Variance of estimate of $\pi$ using AREA')
    ax.grid()

    df = pd.read_csv(args.infile)
    idx = np.array(df.columns, dtype=np.uint)

    # plot trace with semilog-X axis in base 10
    print("\tPlot data from {}".format(args.infile))
    ax.loglog(idx, 4*np.pi*(1-np.pi/4)/idx, '--', c='red', label=r'4$\pi$(1-$\pi$/4)/n')
    ax.loglog(idx, df.var(), '.-', c='blue', label='computed')
    ax.legend()

    print("Saving figure to disk")
    fig.savefig('pi_var.png')
