#!/usr/bin/env bash

# arg1: the heron executable
# arg2: arguments to executable

#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00

mpirun -np $1 --hostfile conf/slurmmpi/nodes java $2 -cp $3 edu.iu.dsc.tws.rsched.schedulers.slurmmpi.MPIProcess --container_class $4 --twister2_home $5 --cluster_name slurmmpi --config_dir $6 &

echo $SLURM_JOB_ID > slurm-job.pid

wait
