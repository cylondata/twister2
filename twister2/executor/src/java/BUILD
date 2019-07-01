package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/common/src/java:common-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
    "//twister2/data/src/main/java:data-java",
    "//twister2/checkpointing/src/java:checkpointing-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/checkpointing:checkpointing-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/util:api-utils-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/task:task-api-java",
    "//twister2/proto:proto-java",
    "@com_google_guava_guava//jar",
]

t2_java_lib(
    name = "executor-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)
