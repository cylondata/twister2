package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "restarter-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Restarter",
    deps = [
        "//twister2/api/src/java:api-java",
        "//twister2/checkpointing/src/java:checkpointing-java",
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/proto:proto-java",
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    ],
)
