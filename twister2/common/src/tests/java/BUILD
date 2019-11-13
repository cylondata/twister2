load("//tools/rules:java_tests.bzl", "java_tests")

java_library(
    name = "common-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//third_party/java:junit4",
        "//third_party/java:mockito",
        "//third_party/java:powermock",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/common/src/java:common-java",
        "//twister2/proto:proto-java",
        "@com_google_protobuf//:protobuf_java",
        "@junit_junit//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)

java_tests(
    size = "small",
    test_classes = [
        "edu.iu.dsc.tws.common.net.tcp.request.TwoChannelTest",
        "edu.iu.dsc.tws.common.util.IterativeLinkedListTest",
    ],
    runtime_deps = [
        ":common-tests",
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
