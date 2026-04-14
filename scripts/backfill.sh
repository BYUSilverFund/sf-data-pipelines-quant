#!/bin/bash

#SBATCH --time=10:00:00   # walltime
#SBATCH --ntasks=16   # number of processor cores (i.e. tasks)
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem-per-cpu=8192M   # memory per CPU core
#SBATCH -J "Backfill"   # job name
#SBATCH --mail-user=stiten@byu.edu   # email address
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

source .venv/bin/activate

python -m pipelines barra backfill --database production --end 2025-12-31
python -m pipelines barra update --database production
python -m pipelines signals backfill --database production
