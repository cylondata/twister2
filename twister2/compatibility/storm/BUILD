package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "//twister2/common/src/java:common-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/master/src/java:master-java",
    "//twister2/api/src/java:api-java",
    "@com_esotericsoftware_kryo//jar",
]

t2_java_lib(
    name = "twister2-storm",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Storm Compatibility",
    deps = common_deps_files,
)
