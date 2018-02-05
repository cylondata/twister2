package(default_visibility = ["//visibility:public"])

java_library(
    name = "resource-scheduler-java",
    srcs = glob(["**/*.java"]),
    deps = [
        "//third_party:ompi_javabinding_java",
        "//twister2/common/src/java:common-java",
        "//twister2/proto:proto-resource-scheduler-java",
        "//twister2/proto:proto_job_java",
        "//twister2/proto:proto_job_state_java",
        "//twister2/proto:proto_resource_scheduler_java",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "@commons_cli_commons_cli//jar",
        "@commons_io_commons_io//jar",
        "@org_apache_commons_commons_compress//jar",
        "@org_apache_curator_curator_client//jar",
        "@org_apache_curator_curator_framework//jar",
        "@org_apache_curator_curator_recipes//jar",
        "@org_apache_zookeeper_zookeeper//jar",
    ],
)
