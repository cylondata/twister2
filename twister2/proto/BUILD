package(default_visibility = ["//visibility:public"])

load("/tools/rules/genproto", "proto_library")

proto_library(
    name = "proto_common",
    src = "common.proto",
    includes = ["twister2/proto"],
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
)

proto_library(
    name = "proto_job",
    src = "job.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
    includes = ["twister2/proto"],
    deps = [
        ":proto_common"
    ],
)

proto_library(
    name = "proto_resource_scheduler",
    src = "resource-scheduler.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
    includes = ["twister2/proto"],
    deps = [
        ":proto_common"
    ],
)

java_library(
    name = "proto-resource-scheduler-java",
    srcs = [
        "Empty.java",
    ],
    deps = [
        "@com_google_protobuf_protobuf_java//jar",
        ":proto_resource_scheduler_java",
        ":proto_job_java",
        ":proto_common_java",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
