package(default_visibility = ["//visibility:public"])

java_library(
    name = "master-java",
    srcs = glob(["**/*.java"]),
    deps = [
        "//twister2/common/src/java:common-java",
        "//twister2/proto:proto_network_java",
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
        "@com_google_protobuf_protobuf_java//jar",
    ],
)
