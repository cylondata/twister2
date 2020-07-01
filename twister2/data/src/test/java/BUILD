package(default_visibility = ["//visibility:public"])

load("//tools/rules:twister2_client.bzl", "twister2_client_common_files")

#java_test(
#    name = "data-java-test",
#    srcs = glob(["*.java"]),
#)

java_library(
    name = "data-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//third_party/java:junit4",
        "//twister2/api/src/java:api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/dataset:dataset-api-java",
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/data/src/main/java:data-java",
        "@com_google_guava_guava",
        "@commons_logging_commons_logging//jar",
        "@org_apache_hadoop_hadoop_hdfs_client",
        "@maven//:org_apache_arrow_arrow_vector",
        "@maven//:org_apache_arrow_arrow_memory",
        "@maven//:org_apache_arrow_arrow_format",
        "@maven//:com_google_flatbuffers_flatbuffers_java",
    ],
)

java_tests(
    classpath_resources = twister2_client_common_files(),
    test_classes = [
        "edu.iu.dsc.tws.data.DiskBackedCollectionPartitionTest",
        #"edu.iu.dsc.tws.data.HDFSBackedCollectionPartitionTest",
    ],
    runtime_deps = [
        ":data-tests",
    ],
)

#java_tests_debug(
#    classpath_resources = twister2_client_common_files(),
#    test_classes = [
#        "edu.iu.dsc.tws.data.DiskBackedCollectionPartitionTest",
#        #"edu.iu.dsc.tws.data.HDFSBackedCollectionPartitionTest",
#    ],
#    runtime_deps = [
#        ":data-tests",
#    ],
#)

#filegroup(
#    name = "srcs",
#    srcs = ["BUILD"] + glob(["**/*.java"]),
#)
