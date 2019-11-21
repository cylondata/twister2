package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "config-api-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 Configuration API",
    deps = [
      "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
    ],
)
