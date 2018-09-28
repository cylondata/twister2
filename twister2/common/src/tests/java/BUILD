load("//tools:rules/java_tests.bzl", "java_tests")

common_deps_files = [
    "//twister2/common/src/java:common-java",
    "//twister2/proto:proto-jobmaster-java",
    "//twister2/proto:proto_jobmaster_java",
    "@org_yaml_snakeyaml//jar",
    "@com_google_protobuf_protobuf_java//jar",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
    "//third_party/java:powermock",
]

java_library(
    name = "common-tests",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
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
