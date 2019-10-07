package(default_visibility = ["//visibility:public"])

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
        "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/dataset:dataset-api-java",
        "//twister2/comms/src/java:comms-java",
    ],
)

java_tests(
    test_classes = [
        "edu.iu.dsc.tws.data.DiskBackedCollectionPartitionTest",
    ],
    runtime_deps = [
        ":data-tests",
    ],
)

#filegroup(
#    name = "srcs",
#    srcs = ["BUILD"] + glob(["**/*.java"]),
#)
