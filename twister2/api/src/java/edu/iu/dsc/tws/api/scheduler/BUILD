package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "scheduler-api-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 Resource API",
    deps = [
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
        "//twister2/proto:proto-java",
        "//twister2/proto/utils:proto-utils-java",
    ],
)
