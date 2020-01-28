package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib", "t2_proto_java_lib")

t2_java_lib(
    name = "proto-utils-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Proto Utils",
    deps = [
        "//twister2/proto:proto-java",
        "@com_google_protobuf_protobuf_java//jar"
    ],
)
