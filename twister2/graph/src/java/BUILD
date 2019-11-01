package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/common/src/java:common-java",
    "//twister2/connectors/src/java:connector-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/taskscheduler/src/java:taskscheduler-java",
    "//twister2/proto:proto-java",
    "//twister2/api/src/java:api-java",
    "//twister2/data/src/main/java:data-java",
    "//twister2/executor/src/java:executor-java",
    "//twister2/master/src/java:master-java",
    "//third_party:ompi_javabinding_java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/checkpointing:checkpointing-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/dataset:dataset-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/scheduler:scheduler-api-java",
    "//twister2/proto/utils:proto-utils-java",
]

t2_java_lib(
    name = "java-graph",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Examples",
    deps = common_deps_files,
)
