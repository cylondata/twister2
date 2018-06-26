#!/usr/bin/env bash

em="\"\""
cp=""
if [ $em != $2 ]; then
    cp = $2
fi

if [ $OMPI_COMM_WORLD_RANK = "0" ]; then
    profile=-agentpath:/home/supun/tools/jprofiler7/bin/linux-x64/libjprofilerti.so=port=8849,nowait
    debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006
fi

profile=
debug=

java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=nodesmpi/logger.properties $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.mpi.MPIWorker --container_class $4 --job_name $5 --twister2_home $6 --cluster_type nodesmpi --config_dir $7 &