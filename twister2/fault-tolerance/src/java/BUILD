package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "fault-tolerance-java",
    srcs = glob(["**/ftolerance/**/*.java"]),
    artifact_name = "Twister2 Fault Tolerance",
    deps = [
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/proto:proto-java",
    ],
)
