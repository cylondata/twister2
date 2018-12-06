package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "api-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 API",
    deps = [
        "//third_party:harp_collective",
        "//third_party:ompi_javabinding_java",
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/executor/src/java:executor-java",
        "//twister2/proto:proto_jobmaster_java",
        "//twister2/proto:proto_job_java",
        "//twister2/proto:proto_job_state_java",
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
        "//twister2/task/src/main/java:task-java",
        "//twister2/taskscheduler/src/java:taskscheduler-java",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "@commons_cli_commons_cli//jar",
        "@it_unimi_dsi_fastutil//jar",
    ],
)
