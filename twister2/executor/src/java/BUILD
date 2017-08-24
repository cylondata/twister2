package(default_visibility = ["//visibility:public"])

#load("/tools/rules/genproto", "proto_library")

#proto_library(
#    name = "executor_schedule",
#    src = "schedule.executor",
#    gen_cc = 0,
#    gen_java = 1,
#    gen_py = 0,
#    includes = ["twister2/executor"],
#)

java_library(
    name = "executor-java",
    srcs = glob(["**/*.java"]),
    deps = [ ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
