package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "checkpointing-java",
    srcs = glob(["**/checkpointing/**/*.java"]),
    artifact_name = "Twister2 Fault Tolerance",
    deps = [
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/proto:proto-java",
        "@com_google_protobuf_protobuf_java",
    ],
)
