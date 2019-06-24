package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "api-utils-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 API Utils",
    deps = [
        "@com_esotericsoftware_kryo//jar",
    ],
)
