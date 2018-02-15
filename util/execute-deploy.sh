#!/bin/sh

set -eu

readonly MVN_GOAL="$1"
readonly VERSION_NAME="$2"
shift 2
readonly EXTRA_MAVEN_ARGS=("$@")

python $(dirname $0)/maven/generate_poms.py $VERSION_NAME \
  //twister2/common/src/java:config-java \
  //twister2/data/src/main/java:data-java \
  //twister2/common/src/java:common-java \
  //twister2/comms/src/java:comms-java \
  //twister2/api/src/java:api-java \
  //twister2/resource-scheduler/src/java:resource-scheduler-java \
  //twister2/proto:proto-resource-scheduler-java \
  //twister2/proto:proto_resource_scheduler_java \
  //twister2/proto:proto-taskscheduleplan-java \
  //twister2/proto:proto_job_java \
  //twister2/proto:proto_job_state_java \
  //twister2/proto:proto_common_java


library_output_file() {
  library=$1
  library_output=bazel-bin/$library
  if [[ ! -e $library_output ]]; then
     library_output=bazel-genfiles/$library
  fi
  if [[ ! -e $library_output ]]; then
    echo "Could not find bazel output file for $library"
    exit 1
  fi
  echo -n $library_output
}

deploy_library() {
  library=$1
  srcjar=$2
  javadoc=$3
  pomfile=$4
  echo bazel build --config=ubuntu $library $srcjar $javadoc
  bazel build --config=ubuntu $library $srcjar $javadoc
  echo mvn -e $MVN_GOAL -Dfile=$(library_output_file $library) -DpomFile=$pomfile "${EXTRA_MAVEN_ARGS[@]:+${EXTRA_MAVEN_ARGS[@]}}"
  mvn -e $MVN_GOAL \
    -Dfile=$(library_output_file $library) \
    -DpomFile=$pomfile \
    "${EXTRA_MAVEN_ARGS[@]:+${EXTRA_MAVEN_ARGS[@]}}"
}

deploy_library \
  twister2/data/src/main/java/libdata-java.jar \
  twister2/data/src/main/java/libdata-java.jar \
  twister2/data/src/main/java/libdata-java.jar \
  data.pom.xml

deploy_library \
  twister2/common/src/java/libcommon-java.jar \
  twister2/common/src/java/libcommon-java.jar \
  twister2/common/src/java/libcommon-java.jar \
  common.pom.xml

deploy_library \
  twister2/common/src/java/libconfig-java.jar \
  twister2/common/src/java/libconfig-java.jar \
  twister2/common/src/java/libconfig-java.jar \
  config.pom.xml

deploy_library \
  twister2/comms/src/java/libcomms-java.jar \
  twister2/comms/src/java/libcomms-java.jar \
  twister2/comms/src/java/libcomms-java.jar \
  comms.pom.xml

deploy_library \
  twister2/resource-scheduler/src/java/libresource-scheduler-java.jar \
  twister2/resource-scheduler/src/java/libresource-scheduler-java.jar \
  twister2/resource-scheduler/src/java/libresource-scheduler-java.jar \
  resource-scheduler.pom.xml

deploy_library \
  twister2/api/src/java/libapi-java.jar \
  twister2/api/src/java/libapi-java.jar \
  twister2/api/src/java/libapi-java.jar \
  api.pom.xml

deploy_library \
  twister2/proto/libproto-resource-scheduler-java.jar \
  twister2/proto/libproto-resource-scheduler-java.jar \
  twister2/proto/libproto-resource-scheduler-java.jar \
  proto-resource-scheduler.pom.xml

deploy_library \
  twister2/proto/libproto_resource_scheduler_java.jar \
  twister2/proto/libproto_resource_scheduler_java.jar \
  twister2/proto/libproto_resource_scheduler_java.jar \
  proto_resource_scheduler.pom.xml

deploy_library \
  twister2/proto/libproto-taskscheduleplan-java.jar \
  twister2/proto/libproto-taskscheduleplan-java.jar \
  twister2/proto/libproto-taskscheduleplan-java.jar \
  proto-taskscheduleplan.pom.xml

deploy_library \
  twister2/proto/libproto_job_java.jar \
  twister2/proto/libproto_job_java.jar \
  twister2/proto/libproto_job_java.jar \
  proto_job.pom.xml

deploy_library \
  twister2/proto/libproto_job_state_java.jar \
  twister2/proto/libproto_job_state_java.jar \
  twister2/proto/libproto_job_state_java.jar \
  proto_job_state.pom.xml

deploy_library \
  twister2/proto/libproto_common_java.jar \
  twister2/proto/libproto_common_java.jar \
  twister2/proto/libproto_common_java.jar \
  proto_common.pom.xml