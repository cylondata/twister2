package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/common/src/java:common-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/taskscheduler/src/java:taskscheduler-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/data/src/main/java:data-java",
    "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    "//twister2/master/src/java:master-java",
    "//twister2/proto:proto_jobmaster_java",
    "@com_google_guava_guava//jar",
]

t2_java_lib(
    name = "executor-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)
