load("//tools/rules:java_tests.bzl", "java_tests")

java_library(
    name = "deeplearning-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//third_party/java:junit4",
        "//third_party/java:mockito",
        "//third_party/java:powermock",
        "//twister2/common/src/java:common-java",
        "//twister2/task/src/main/java:task-java",
        "//twister2/master/src/java:master-java",
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
        "@junit_junit//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)

java_tests(
    size = "small",
    test_classes = [
        "edu.iu.dsc.tws.deeplearning.io.TestReadCSV",
        "edu.iu.dsc.tws.deeplearning.util.TestFileUtils",
    ],
    runtime_deps = [
        ":deeplearning-tests",
    ],
)

#java_tests_debug(
#    size = "enormous",
#    test_classes = [
#        "edu.iu.dsc.tws.common.net.tcp.request.TwoChannelTest",
#        "edu.iu.dsc.tws.common.util.IterativeLinkedListTest",
#    ],
#    runtime_deps = [
#        ":common-tests",
#    ],
#)
