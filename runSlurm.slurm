#!/bin/bash
#SBATCH -J ABM
#SBATCH --mem=90G
#SBATCH --cpus-per-task=32
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=EMAIL
#SBATCH -C "ceph"

. /usr/modules/init/bash
module load julia

cd $SLURM_SUBMIT_DIR

julia -p 32 $FILE
