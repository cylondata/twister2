#!/usr/bin/env bash

#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00

em="\"\""
cp=""
if [ $em != $2 ]; then
    cp = $2
fi

$8 java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp ${9} ${10} -Djava.util.loggi.config.file=common/logger.properties $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker --container_class $4 --job_id $5 --twister2_home $6 --cluster_type slurm --config_dir $7 --job_master_port ${11} --job_master_ip ${12} &

wait
