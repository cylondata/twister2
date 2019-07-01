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
        "//twister2/api/src/java/edu/iu/dsc/tws/api/task:task-api-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/task/src/main/java:task-java",
        "@com_google_guava_guava",
    ],
)
