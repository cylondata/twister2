#!/usr/bin/env bash

em="\"\""
c=$2

profile=
debug=

echo $c

echo "java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=nodesmpi/logger.properties -cp $2 edu.iu.dsc.tws.rsched.schedulers.standalone.StandaloneWorker --container_class $3 --job_name $4 --twister2_home $5 --cluster_type nodesmpi --config_dir $6"
java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=nodesmpi/logger.properties -cp $2 edu.iu.dsc.tws.rsched.schedulers.standalone.StandaloneProcess --container_class $3 --job_name $4 --twister2_home $5 --cluster_type nodesmpi --config_dir $6

EXIT_STATUS=0

