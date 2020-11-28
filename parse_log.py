#!/usr/bin/env python3

import argparse
import pandas as pd
# HTCondor libraries
import htcondor

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-l', '--logfile', required=True,
        help="Log file to be processed")
    parser.add_argument('-o', '--outfile', required=True,
        help="Name of output CSV file")
    args = parser.parse_args()

    # Read log file
    jel = htcondor.JobEventLog(args.logfile)

    events = {}
    for event in jel.events(stop_after=0):
        # Unique Job ID
        ID = "{}.{}.{}".format(event['Cluster'], event['Proc'], event['Subproc'])
        # Job Event type
        jet = event['MyType']

        if ID not in events and jet == 'SubmitEvent':
            # Job task is submitted to queue
            events[ID] = {
                'ID': ID, 'JobID': event['LogNotes'],
                'SubmitTime': event['EventTime']                    # UTC Date & Time
            }
        else:
            # Job begins running on Execute Node
            if jet == 'ExecuteEvent':
                events[ID].update({
                    'ExecTime': event['EventTime']                  # UTC Date & Time
                })
            # Transfer out of output files begins
            elif jet == 'FileTransferEvent' and event['Type'] == 5:
                events[ID].update({
                    'TransferOutTime': event['EventTime']           # UTC Date & Time
                })
            # Job is removed from queue
            elif jet == 'JobTerminatedEvent':
                # Sometimes usage doesn't register
                if 'CpusUsage' not in event:
                    event['CpusUsage'] = 'NA'

                events[ID].update({
                    'TermTime': event['EventTime'],                 # UTC Date & Time
                    'TermStatus': event['ReturnValue'],
                    'ElapsedTime': event['TotalRemoteUsage'],       # Seconds: Usr & Sys
                    'MemReq': event['RequestMemory'],               # MB
                    'MemAlloc': event['Memory'],                    # MB
                    'MemUse': event['MemoryUsage'],                 # MB
                    'BytesSent': event['TotalSentBytes'],           # Bytes
                    'BytesReceived': event['TotalReceivedBytes'],   # Bytes
                    'DiskReq': event['RequestDisk'],                # KB
                    'DiskAlloc': event['Disk'],                     # KB
                    'DiskUse': event['DiskUsage'],                  # KB
                    'CpusReq': event['RequestCpus'],
                    'CpusAlloc': event['Cpus'],
                    'CpusUse': event['CpusUsage']
                })

    # Restructure data into tabular format
    logs = pd.DataFrame.from_dict(events.values())
    logs.to_csv(args.outfile, index=False)
