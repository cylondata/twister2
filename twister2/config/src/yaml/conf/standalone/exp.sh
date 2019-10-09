#!/usr/bin/env bash

em="\"\""
cp=""
if [ $em != $2 ]; then
  cp = $2
fi

if [ $OMPI_COMM_WORLD_RANK = "0" ]; then
    profile=-agentpath:/home/skamburu/tools/jprofiler10/bin/linux-x64/libjprofilerti.so=port=8849
    debug=
    #if 15th arg is debug them enable debug
    if [ "${15}" = "debug" ]; then
        debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006
    fi
fi

profile=

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

# set the lib path to bundled mpi, if that mpirun is used
lib_path="-Djava.library.path=${LD_LIBRARY_PATH}"
if [ "$9" = "twister2-core/ompi/bin/mpirun" ]; then
  lib_path="${lib_path}:${17}/ompi/lib"
fi

illegal_access_warn=
if [ "${16}" = "suppress_illegal_access_warn" ]; then
  illegal_access_warn="--illegal-access=deny"
fi

$JAVA_HOME/bin/java $illegal_access_warn $lib_path $debug $profile -Djava.util.logging.config.file=common/logger.properties -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp ${10} ${11} $cp -cp $3 edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker --container_class $4 --job_name $5 --twister2_home $6 --cluster_type standalone --config_dir $7 --job_master_port ${12} --job_master_ip ${13} &
