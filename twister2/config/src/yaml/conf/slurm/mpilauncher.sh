#!/usr/bin/env bash

#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00

$6 java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp \
   ${7} ${8} \
   -Djava.util.logging.config.file=common/logger.properties \
   -cp $2 \
   edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorkerStarter \
   --job_id $3 \
   --twister2_home $4 \
   --cluster_type slurm \
   --config_dir $5 \
   --job_master_port ${9} \
   --job_master_ip ${10} \
   --restore_job ${11} \
   --restart_count ${12}
