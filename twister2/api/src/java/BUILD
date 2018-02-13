package(default_visibility = ["//visibility:public"])

java_library(
    name = "api-java",
    srcs = glob(["**/*.java"]),
    deps = [
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
        "@commons_cli_commons_cli//jar",
        "//twister2/proto:proto-resource-scheduler-java",
        "//third_party:ompi_javabinding_java",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "//twister2/proto:proto_job_state_java",
        "//twister2/proto:proto_job_java",
        "//twister2/proto:proto_resource_scheduler_java"]
)


