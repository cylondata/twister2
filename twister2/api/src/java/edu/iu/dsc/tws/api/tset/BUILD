package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "tset-api-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 Task API",
    deps = [
        "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/dataset:dataset-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/task/src/main/java:task-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
        "@org_apache_hadoop_hadoop_mapreduce_client_core//jar",
        "@org_apache_hadoop_hadoop_common//jar",
        "@com_google_guava_guava",
        "@com_google_protobuf//:protobuf_java",
        "@maven//:org_apache_arrow_arrow_vector",
        "@maven//:org_apache_arrow_arrow_memory",
        "@maven//:org_apache_arrow_arrow_format",
        "@maven//:com_google_flatbuffers_flatbuffers_java",
    ],
)
