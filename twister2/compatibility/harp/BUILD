package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "//twister2/proto:proto-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
    "//third_party:harp_collective",
]

t2_java_lib(
    name = "twister2-harp",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Harp Compatibility",
    deps = common_deps_files,
)
