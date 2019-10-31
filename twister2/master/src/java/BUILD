package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "master-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Master",
    deps = [
        "//twister2/api/src/java/edu/iu/dsc/tws/api/checkpointing:checkpointing-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/faulttolerance:fault-tolerance-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/net:network-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
        "//twister2/checkpointing/src/java:checkpointing-java",
        "//twister2/common/src/java:common-java",
        "//twister2/proto:proto-java",
        "//twister2/proto/utils:proto-utils-java",
        "@com_google_protobuf_protobuf_java//jar",
        "@javax_ws_rs_javax_ws_rs_api//jar",
        "@maven//:org_apache_curator_curator_framework",
    ],
)
