package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "//twister2/common/src/java:common-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/master/src/java:master-java",
    "//twister2/proto:proto-java",
    "//twister2/api/src/java:api-java",
    "//twister2/executor/src/java:executor-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/data/src/main/java:data-java",
    "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
    "//third_party:ompi_javabinding_java",
    "@com_esotericsoftware_kryo//jar",
    "@com_google_guava_guava//jar",
    "@org_lmdbjava_lmdbjava//jar",
    "@org_apache_commons_commons_lang3//jar",
    "@commons_cli_commons_cli//jar",
    "@com_google_protobuf//:protobuf_java",
    "@commons_io_commons_io//jar",
]

t2_java_lib(
    name = "twister2-deeplearning",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 DeepLearning",
    deps = common_deps_files,
)