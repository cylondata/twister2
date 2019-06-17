package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "//twister2/common/src/java:common-java",
    "//twister2/proto:proto-java",
    "//third_party:harp_collective",
]

t2_java_lib(
    name = "twister2-harp",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Harp Compatibility",
    deps = common_deps_files,
)
