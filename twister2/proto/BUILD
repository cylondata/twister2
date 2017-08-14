package(default_visibility = ["//visibility:public"])

load("/tools/rules/genproto", "proto_library")

proto_library(
    name = "proto_schedule",
    src = "schedule.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
    includes = ["twister2/proto"],
)

java_library(
    name = "proto-java",
    srcs = [
        "Empty.java",
    ],
    deps = [
        "@com_google_protobuf_protobuf_java//jar",
        ":proto_schedule_java",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
