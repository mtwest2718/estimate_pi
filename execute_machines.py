#!/usr/bin/env python3

import pandas as pd
# HTCondor libraries
import htcondor

coll = htcondor.Collector()
# Get information about all machines listed in Collector
machine_ads = coll.query(
    htcondor.AdTypes.Any,
    constraint='MyType=="Machine"',
    projection=[
        'Name', 'Memory', 'Disk', 'VirtualMemory',
        'Cpus', 'Arch', 'CpuFamily', 'CpuModelNumber',
        'TotalSlots', 'SlotType',
        'OpSysLegacy', 'OpSysLongName', 'CondorVersion',
        'HasMPI',
        'HasFileTransferPluginMethods', "HasPerFileEncryption",
        'LoadAvg', 'CondorLoadAvg', 'State', 'Activity',
        'DaemonStartTime', 'LastHeardFrom'
    ]
)

for machine in machine_ads:
    print("SlotNumber@MachineName: {}".format(machine['Name']))
    for (k, v) in machine.items():
        if k != 'Name':
            print("\t{}: {}".format(k,v))
