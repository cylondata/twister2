#!/usr/bin/env bash

em="\"\""
c=$2

echo $c

if [ $NOMAD_ALLOC_INDEX = "5" ]; then
    profile=-agentpath:/home/supun/tools/jprofiler7/bin/linux-x64/libjprofilerti.so=port=8849,nowait
    debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006
fi

echo $NOMAD_ALLOC_INDEX
echo $NOMAD_ALLOC_ID
echo $debug

# download the package
cp ${CORE_PACKAGE_ENV} .
cp ${JOB_PACKAGE_ENV} .

tar -xvf twister2-core.tar.gz
tar -xvf twister2-job.tar.gz --strip 1

profile=
#debug=

ls

cp="*:twister2-core/lib/*"
echo $cp
echo "1" $1
echo "2" $2
echo "3" $3
echo "4" $4
echo "5" $5
echo "6" $6
#echo "java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=nodesmpi/logger.properties -cp $2 edu.iu.dsc.tws.rsched.schedulers.standalone.StandaloneWorkerStarter --container_class $3 --job_name $4 --twister2_home $5 --cluster_type nodesmpi --config_dir $6"
java $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.util.logging.config.file=standalone/logger.properties -cp $cp edu.iu.dsc.tws.rsched.schedulers.standalone.StandaloneWorkerStarter --container_class $4 --job_name $5 --twister2_home $6 --cluster_type standalone --config_dir $7 2>&1 | tee out.txt

cat out.txt

EXIT_STATUS=0

