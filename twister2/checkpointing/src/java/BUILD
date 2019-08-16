package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "checkpointing-java",
    srcs = glob(["**/checkpointing/**/*.java"]),
    artifact_name = "Twister2 Fault Tolerance",
    deps = [
        "//twister2/api/src/java/edu/iu/dsc/tws/api/checkpointing:checkpointing-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/net:network-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/proto:proto-java",
        "@com_google_protobuf_protobuf_java",
        "@org_apache_hadoop_hadoop_common",
    ],
)
