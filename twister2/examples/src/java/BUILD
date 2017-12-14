package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/common/src/java:common-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/api/src/java:api-java",
    "//twister2/data/src/main/java:data-java",
   ]

java_library(
    name = "examples-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)
