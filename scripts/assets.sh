#!/bin/bash

#SBATCH --time=10:00:00   # walltime
#SBATCH --ntasks=16   # number of processor cores (i.e. tasks)
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem-per-cpu=16384M   # memory per CPU core
#SBATCH -J "Backfill"   # job name
#SBATCH --mail-user=amh1124@byu.edu   # email address
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

source .venv/bin/activate

python pipelines/assets_flow.py