package(default_visibility = ["//visibility:public"])

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

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
