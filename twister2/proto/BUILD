package(default_visibility = ["//visibility:public"])

load("//tools:rules/genproto.bzl", "proto_library")

proto_library(
    name = "proto_job_state",
    src = "job_state.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
    includes = ["twister2/proto"],
)

proto_library(
    name = "proto_job",
    src = "job.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
    includes = ["twister2/proto"],
)

proto_library(
    name = "proto_taskscheduleplan",
    src = "taskscheduleplan.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
    includes = ["twister2/proto"],
)

java_library(
    name = "proto-taskscheduleplan-java",
    srcs = [
        "Empty.java",
    ],
    deps = [
        ":proto_job_java",
        ":proto_job_state_java",
        ":proto_taskscheduleplan_java",
        "@com_google_protobuf_protobuf_java//jar",
    ],
)

proto_library(
    name = "proto_jobmaster",
    src = "jobmaster.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
    includes = ["twister2/proto"],
)

proto_library(
    name = "proto_checkpoint",
    src = "checkpoint.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
    includes = ["twister2/proto"],
)

java_library(
    name = "proto-jobmaster-java",
    srcs = [
        "Empty.java",
    ],
    deps = [
        ":proto_jobmaster_java",
        "@com_google_protobuf_protobuf_java//jar",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
