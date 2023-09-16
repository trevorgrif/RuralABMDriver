#!/bin/bash
#SBATCH -J ABM
#SBATCH --mem=100G
#SBATCH --cpus-per-task=32
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=EMAIL
#SBATCH -C "ceph"

. /usr/modules/init/bash
module load julia

cd $SLURM_SUBMIT_DIR

julia -t 32 -p 32 -L distributed_startup.jl $FILE
