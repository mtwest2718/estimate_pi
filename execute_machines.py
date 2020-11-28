#!/usr/bin/env python3

import pandas as pd
# HTCondor libraries
import htcondor

# Desired classad keys for Execute Machine nodes
classad_keys = [
    'Name', 'Memory', 'Disk', 'VirtualMemory',
    'Cpus', 'Arch', 'CpuFamily', 'CpuModelNumber',
    'OpSysLegacy', 'OpSysLongName', 'UtsnameRelease', 'UtsnameVersion',
    'TotalSlots', 'SlotType', 'CondorVersion',
    'HasMPI', 'HasFileTransferPluginMethods', "HasPerFileEncryption",
    'LoadAvg', 'CondorLoadAvg', 'State', 'Activity',
    'DaemonStartTime', 'LastHeardFrom'
]

coll = htcondor.Collector()
# Get information about all execute machines listed in Collector
machine_ads = coll.query(
    htcondor.AdTypes.Any,
    constraint='MyType=="Machine" && TargetType=="Job"',
    projection=classad_keys
)

for machine in machine_ads:
    print("SlotName@MachineName: {}".format(machine['Name']))
    for key in classad_keys:
        if key != 'Name' and key in machine:
            print("\t{}: {}".format(key, machine[key]))
