#!/usr/bin/env python3
import argparse
import numpy as np
import pandas as pd
import pdb
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--infiles', nargs='+', help="list of input files")
    parser.add_argument('--outfile', help="the output file name")
    args = parser.parse_args()

    # summary output file setup
    df = pd.read_csv(arg.infiles[0])
    sample_idx = np.array(
        2**np.arange(0, np.floor(np.log2(len(df)))+1), dtype=np.uint)
    summ_array = np.zeros([len(arg.infiles), len(sample_idx)], dtype=np.single)

    for m, csvfile in enumerate(args.infiles):
        df = pd.read_csv(csvfile, usecols=['est_area'])
        # subset of estimates where #-samples is an power of 2
        summ_array[m,:] = df.iloc[sample_idx-1].to_numpy()[:,0]

    # Convert to a pandas DataFrame with index labels
    summ_df = pd.DataFrame(summ_array, columns=list(map(str,sample_idx)))
    samples.to_csv(args.outfile, index=False)
