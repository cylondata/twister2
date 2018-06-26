#!/usr/bin/env bash

#SBATCH --ntasks-per-node=1+1166665
#SBATCH --time=00:30:00

em="\"\""
cp=""
if [ $em != $2 ]; then
    cp = $2
fi

echo mpirun -np $1 --hostfile $8 java -Djava.util.loggi.config.file=conf/logger.properties $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.slurmmpi.MPIWorker --container_class $4 --job_name $5 --twister2_home $6 --cluster_type nodesmpi --config_dir $7
mpirun java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.loggi.config.file=slurmmpi/logger.properties $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.mpi.MPIWorker --container_class $4 --twister2_home $5 --cluster_type slurmmpi --config_dir $6 &

wait
