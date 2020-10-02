#!/usr/bin/env bash
######################################################################
# This is the launcher script for standalone Twister2 Workers
######################################################################

if [ "$OMPI_COMM_WORLD_RANK" = "0" ]; then
    profile=-agentpath:/home/skamburu/tools/jprofiler10/bin/linux-x64/libjprofilerti.so=port=8849
    debug=
    #if 15th arg is debug them enable debug
    if [ "$DEBUG" = "debug" ]; then
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
    echo "Please specify JAVA_HOME. in conf/standalone/mpiworker.sh or as system-wide JAVA_HOME."
    exit 1
else
    JAVA_HOME=${CUSTOM_JAVA_HOME}
fi

illegal_access_warn=
if [ "${ILLEGAL_ACCESS_WARN}" = "suppress_illegal_access_warn" ]; then
  illegal_access_warn="--illegal-access=deny"
fi

CLASSPATH=${CLASSPATH}:${SUBMITTING_TWISTER2_HOME}/lib

export UCX_TCP_CM_ALLOW_ADDR_INUSE=y

$JAVA_HOME/bin/java $illegal_access_warn $debug $profile \
   -Djava.util.logging.config.file=common/logger.properties \
   -Djava.library.path=${LD_LIBRARY_PATH} \
   -Xmx${XMX_VALUE} \
   -Xms${XMS_VALUE} \
   edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorkerStarter \
    --job_id $JOB_ID \
    --twister2_home $TWISTER2_HOME \
    --cluster_type standalone \
    --config_dir $CONFIG_DIR \
    --job_master_port ${JOB_MASTER_PORT} \
    --job_master_ip ${JOB_MASTER_IP} \
    --restore_job ${RESTORE_JOB} \
    --restart_count ${RESTART_COUNT}
