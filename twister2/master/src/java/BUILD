package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "master-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Master",
    deps = [
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/proto:proto_checkpoint_java",
        "//twister2/proto:proto_job_java",
        "//twister2/proto:proto_job_state_java",
        "//twister2/proto:proto_jobmaster_java",
        "//twister2/task/src/main/java:task-java",
        "@com_google_protobuf//:protobuf_java",
    ],
)
