#!/usr/bin/env bash

em="\"\""
cp=""
if [ $em != $2 ]; then
    cp = $2
fi
debug="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
# echo mpirun -np $1 --hostfile $8 java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.loggi.config.file=nodesmpi/logger.properties $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.mpi.MPIWorker --container_class $4 --job_name $5 --twister2_home $6 --cluster_type nodesmpi --config_dir $7

mpirun -np $1 --hostfile $8 sh `pwd`/nodesmpi/exp.sh "$@"

echo $SLURM_JOB_ID > slurm-job.pid

wait
