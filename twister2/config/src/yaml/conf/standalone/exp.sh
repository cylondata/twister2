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

# set JAVA_HOME by user
CUSTOM_JAVA_HOME=
# check if config specified JAVA_HOME
if [ -z "${CUSTOM_JAVA_HOME}" ]; then
    # config did not specify JAVA_HOME. Use system JAVA_HOME
    CUSTOM_JAVA_HOME=${JAVA_HOME}
fi
# check if we have a valid JAVA_HOME and if java is not available
if [ -z "${CUSTOM_JAVA_HOME}" ] && ! type java > /dev/null 2> /dev/null; then
    echo "Please specify JAVA_HOME. in conf/standalone/exp.sh or as system-wide JAVA_HOME."
    exit 1
else
    JAVA_HOME=${CUSTOM_JAVA_HOME}
fi

$JAVA_HOME/bin/java -Djava.library.path=twister2-core/ompi/lib $debug $profile -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp $10 $11 -Djava.util.logging.config.file=standalone/logger.properties $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker --container_class $4 --job_name $5 --twister2_home $6 --cluster_type standalone --config_dir $7 --job_master_port $12 --job_master_ip $13 &