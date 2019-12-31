package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "//twister2/common/src/java:common-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/master/src/java:master-java",
    "//twister2/api/src/java:api-java",
    "//twister2/executor/src/java:executor-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/data/src/main/java:data-java",
    "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
    "@maven//:com_google_code_gson_gson",
    "@com_esotericsoftware_kryo//jar",
]

t2_java_lib(
    name = "twister2-storm",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Storm Compatibility",
    deps = common_deps_files,
)
