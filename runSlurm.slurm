#!/bin/bash
#SBATCH -J ABM
#SBATCH --mem=120G
#SBATCH --cpus-per-task=16
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=EMAIL
#SBATCH -C "ceph"

. /usr/modules/init/bash
module load julia

cd $SLURM_SUBMIT_DIR

julia -p 15 $FILE
