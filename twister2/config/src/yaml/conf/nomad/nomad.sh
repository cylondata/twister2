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
#wget http://149.165.150.81:8082/twister2/mesos/twister2-job.tar.gz .
#wget http://149.165.150.81:8082/twister2/mesos/twister2-core-0.4.0.tar.gz .

method=$10

#echo ${CORE_PACKAGE_ENV}
#echo ${JOB_PACKAGE_ENV}

if [ $method = "LOCAL" ]; then
  cp $11/twister2-job.tar.gz .
  cp $11/twister2-core-0.4.0.tar.gz .
else
  wget $8 .
  wget $9 .
fi

tar -xvf twister2-core-0.4.0.tar.gz
tar -xvf twister2-job.tar.gz --strip 1

profile=
debug=

ls
cp="*:twister2-core/lib/*"
#cp="*:twister2-0.4.0/lib/*"
echo $cp
echo "1" $1
echo "2" $2
echo "3" $3
echo "4" $4
echo "5" $5
echo "6" $6
echo "7" $7
echo "8" $8
echo "9" $9
echo "10" $10
echo "11" $11



#echo "java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=nomad/logger.properties -cp $2 edu.iu.dsc.tws.rsched.schedulers.nomad.NomadWorkerStarter --container_class $3 --job_name $4 --twister2_home $5 --cluster_type nomad --config_dir $6"
if [ $NOMAD_ALLOC_INDEX = "0" ] && [ $1 = "false" ]; then
  java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=common/logger.properties -cp $cp edu.iu.dsc.tws.rsched.schedulers.nomad.NomadJobMasterStarter --container_class $4 --job_name $5 --job_id $7 --twister2_home . --cluster_type nomad --config_dir . 2>&1 | tee out.txt
else
  java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=common/logger.properties -cp $cp edu.iu.dsc.tws.rsched.schedulers.nomad.NomadWorkerStarter --container_class $4 --job_name $5 --twister2_home . --cluster_type nomad --config_dir . 2>&1 | tee out.txt
fi

cat out.txt

EXIT_STATUS=0
