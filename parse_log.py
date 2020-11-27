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
        EventType = event['MyType']

        if ID not in events and EventType == 'SubmitEvent':
            # Job task is submitted to queue
            events[ID] = {'ID': ID, 'JobID': event['LogNotes'], 'SubmitTime': event['EventTime']}
        else:
            # Job begins running on Execute Node
            if EventType == 'ExecuteEvent':
                events[ID].update({'ExecTime': event['EventTime']})
            # Transfer out of output files begins
            elif EventType == 'FileTransferEvent' and event['Type'] == 5:
                events[ID].update({'TransferOutTime': event['EventTime']})
            # Job is removed from queue
            elif EventType == 'JobTerminatedEvent':
                events[ID].update({
                    'TermTime': event['EventTime'], 'TermStatus': event['ReturnValue'],
                    'MemReq': event['Memory'], 'MemUse': event['MemoryUsage'],
                    'DiskReq': event['RequestDisk'], 'DiskUse': event['DiskUsage'],
                    'CpusReq': event['RequestCpus'],
                })

    # Restructure data into tabular format
    logs = pd.DataFrame.from_dict(events.values())
    logs.to_csv(args.outfile, index=False)
