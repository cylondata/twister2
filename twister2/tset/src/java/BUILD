package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "tset-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 TSet Implementation",
    deps = [
        "//twister2/api/src/java/edu/iu/dsc/tws/api/checkpointing:checkpointing-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/dataset:dataset-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/tset:tset-api-java",
        "//twister2/checkpointing/src/java:checkpointing-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/task/src/main/java:task-java",
        "//twister2/proto:proto-java",
        "//twister2/proto/utils:proto-utils-java",
        "//twister2/master/src/java:master-java",
        "@com_google_protobuf//:protobuf_java",
        "@com_google_guava_guava",
        "@org_apache_hadoop_hadoop_common//jar",
        "@org_apache_hadoop_hadoop_mapreduce_client_core//jar",
    ],
)
