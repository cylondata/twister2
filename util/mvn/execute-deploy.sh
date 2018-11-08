#!/bin/bash

set -eu

readonly MVN_GOAL="$1"
readonly VERSION_NAME="$2"
shift 2
readonly EXTRA_MAVEN_ARGS=("$@")

bazel_output_file() {
  local library=$1
  local output_file=bazel-bin/$library
  if [[ ! -e $output_file ]]; then
     output_file=bazel-genfiles/$library
  fi
  if [[ ! -e $output_file ]]; then
    echo "Could not find bazel output file for $library"
    exit 1
  fi
  echo -n $output_file
}

deploy_library() {
  local library=$1
  local pomfile=$2
  bazel build --define=pom_version="$VERSION_NAME" \
    $library $pomfile

  mvn $MVN_GOAL \
    -Dfile=$(bazel_output_file $library) \
    -DpomFile=$(bazel_output_file $pomfile) \
    "${EXTRA_MAVEN_ARGS[@]:+${EXTRA_MAVEN_ARGS[@]}}"
}

deploy_library \
  twister2/api/src/java/libapi-java.jar \
  twister2/api/src/java/pom.xml

deploy_library \
  twister2/common/src/java/libcommon-java.jar \
  twister2/common/src/java/pom.xml

deploy_library \
  twister2/comms/src/java/libcomms-java.jar \
  twister2/comms/src/java/pom.xml

deploy_library \
  twister2/connectors/src/java/libconnector-java.jar \
  twister2/connectors/src/java/pom.xml

deploy_library \
  twister2/data/src/main/java/libdata-java.jar \
  twister2/data/src/main/java/pom.xml

deploy_library \
  twister2/examples/src/java/libexamples-java.jar \
  twister2/examples/src/java/pom.xml

deploy_library \
  twister2/executor/src/java/libexecutor-java.jar \
  twister2/executor/src/java/pom.xml

deploy_library \
  twister2/master/src/java/libmaster-java.jar \
  twister2/master/src/java/pom.xml

deploy_library \
  twister2/resource-scheduler/src/java/libresource-scheduler-java.jar \
  twister2/resource-scheduler/src/java/pom.xml

deploy_library \
  twister2/task/src/main/java/libtask-java.jar \
  twister2/task/src/main/java/pom.xml

deploy_library \
  twister2/taskscheduler/src/java/libtaskscheduler-java.jar \
  twister2/taskscheduler/src/java/pom.xml



