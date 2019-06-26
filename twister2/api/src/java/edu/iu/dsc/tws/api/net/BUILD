package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "network-api-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 Network API",
    deps = [
        "@com_google_protobuf//:protobuf_java",
    ],
)
