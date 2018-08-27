package(default_visibility = ["//visibility:public"])

java_library(
    name = "master-java",
    srcs = glob(["**/*.java"]),
    deps = [
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/proto:proto_checkpoint_java",
        "//twister2/proto:proto_job_state_java",
        "//twister2/proto:proto_jobmaster_java",
        "//twister2/task/src/main/java:task-java",
        "@com_google_protobuf_protobuf_java//jar",
    ],
)
