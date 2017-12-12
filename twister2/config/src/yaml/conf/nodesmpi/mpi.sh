#!/usr/bin/env bash

# arg1: the heron executable
# arg2: arguments to executable

#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00

em="\"\""
cp=""
if [ $em != $2 ]; then
    cp = $2
fi

echo mpirun -np $1 --hostfile $7 java -Djava.util.loggi.config.file=conf/logger.properties $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.slurmmpi.MPIProcess --container_class $4 --twister2_home $5 --cluster_name nodesmpi --config_dir $6
mpirun -np $1 --hostfile $7 java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.loggi.config.file=conf/logger.properties $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.mpi.MPIProcess --container_class $4 --twister2_home $5 --cluster_name nodesmpi --config_dir $6 &

echo $SLURM_JOB_ID > slurm-job.pid

wait
