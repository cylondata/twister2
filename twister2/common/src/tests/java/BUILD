load("//tools:rules/java_tests.bzl", "java_tests")

java_library(
    name = "common-tests",
    srcs = glob(["**/*.java"]),
    deps = [
               "//twister2/common/src/java:common-java",
               "//twister2/proto:proto-java",
               "@org_yaml_snakeyaml//jar",
               "@com_google_protobuf//:protobuf_java",
               "//third_party/java:mockito",
               "//third_party/java:junit4",
               "//third_party/java:powermock",
               "@junit_junit//jar",
           ],
)

java_tests(
    size = "small",
    test_classes = [
        "edu.iu.dsc.tws.common.net.tcp.request.PingTest",
        "edu.iu.dsc.tws.common.net.tcp.request.TwoChannelTest",
    ],
    runtime_deps = [
        ":common-tests",
    ],
)
