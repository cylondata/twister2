package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/common/src/java:config-java",
    "//twister2/common/src/java:common-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/taskscheduler/src/java:taskscheduler-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/data/src/main/java:data-java",
    "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    "//twister2/master/src/java:master-java",
    "@com_google_guava_guava//jar",
]

java_library(
    name = "executor-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)
