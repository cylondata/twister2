#!/usr/bin/env bash

em="\"\""
c=$2

echo $c

#if [ $NOMAD_ALLOC_INDEX = "1" ]; then
#    profile=-agentpath:/home/supun/tools/jprofiler7/bin/linux-x64/libjprofilerti.so=port=8849,nowait
#    debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006
#fi

echo $NOMAD_ALLOC_INDEX
echo $NOMAD_ALLOC_ID
echo $debug

# download the package
wget http://149.165.150.81:8082/twister2/mesos/twister2-core-0.2.1.tar.gz .
wget http://149.165.150.81:8082/twister2/mesos/twister2-job.tar.gz .

tar -xvf twister2-core-0.2.1.tar.gz
tar -xvf twister2-job.tar.gz --strip 1

profile=
debug=

ls

cp="*:twister2-core/lib/*"
echo $cp
echo "1" $1
echo "2" $2
echo "3" $3
echo "4" $4
echo "5" $5
echo "6" $6
#echo "java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=nomad/logger.properties -cp $2 edu.iu.dsc.tws.rsched.schedulers.nomad.NomadWorkerStarter --container_class $3 --job_name $4 --twister2_home $5 --cluster_type nomad --config_dir $6"
java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=nomad/logger.properties -cp $cp edu.iu.dsc.tws.rsched.schedulers.nomad.NomadWorkerStarter --container_class $4 --job_name $5 --twister2_home . --cluster_type nomad --config_dir . 2>&1 | tee out.txt

cat out.txt

EXIT_STATUS=0

