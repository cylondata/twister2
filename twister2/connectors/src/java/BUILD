package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "//twister2/common/src/java:common-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/master/src/java:master-java",
    "@org_xerial_snappy_snappy_java//jar",
    "@org_lz4_lz4_java//jar",
    "@org_slf4j_slf4j_api//jar",
    "@org_apache_kafka_kafka_clients//jar",
]

t2_java_lib(
    name = "connector-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Connectors",
    deps = common_deps_files,
)

filegroup(
    name = "srcs",
    srcs = ["BUILD"] + glob(["**/*.java"]),
)
