package(default_visibility = ["//visibility:public"])

java_library(
    name = "resource-scheduler-java",
    srcs = glob(["**/*.java"]),
    deps = [
        "//twister2/common/src/java:common-java",
        "@commons_cli_commons_cli//jar",
        "//twister2/proto:proto-resource-scheduler-java",
        "//twister2/proto:proto_job_java",
        "//twister2/proto:proto_resource_scheduler_java",
        "//third_party:ompi_javabinding_java",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",]
)