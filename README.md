# High-Throughput Pi Making
This is a short example of how one can use the HTCondor Python API to programmatically construct a multi-step analysis pipeline. The two steps are
- Simulate random numbers to estimate Pi
- Produce trace plots from the collection of output files

I am leveraging a Docker container that automatically starts up a single-node HTCondor pool to run this workflow on my quad-core Windows laptop.

## What is in here?
- Needed stuff
    - `pi_samples.py`: Generates the random samples of Pi
    - `pi_trace.py`: Creates the trace plot
    - `pi_dag.py`: Generates the DAG and submits the workflow
- Diagnostic tools
    - `parse_log.py`: Parses individual log file into CSV table
    - `execute_machines.py`: Gets information about execute machines
- `*_submit.py`: For testing executables without submitting whole DAG

## References
- General [overview](https://htcondor.readthedocs.io/en/latest/overview/index.html)
- User's [manual](https://htcondor.readthedocs.io/en/latest/users-manual/index.html)
- Python API [reference material](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/index.html)
- CLI reference [manual](https://htcondor.readthedocs.io/en/latest/man-pages/index.html)
- HTC-Scipy notebook container [page](https://hub.docker.com/r/htcondor/htc-scipy-notebook)
