package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "test-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 Testbed",
    deps = [
        "@com_fasterxml_jackson_core_jackson_core",
        "@com_fasterxml_jackson_core_jackson_databind",
    ],
)
