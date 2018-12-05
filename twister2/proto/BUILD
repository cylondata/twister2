package(default_visibility = ["//visibility:public"])

#JOB STATE
native.proto_library(
    name = "proto_job_state",
    srcs = ["job_state.proto"],
    proto_source_root = "twister2/proto",
)

native.java_proto_library(
    name = "proto_job_state_java",
    deps = [":proto_job_state"],
)

#JOB
native.proto_library(
    name = "proto_job",
    srcs = ["job.proto"],
    proto_source_root = "twister2/proto",
)

native.java_proto_library(
    name = "proto_job_java",
    deps = [":proto_job"],
)

#TASK SCHEDULE PLAN
native.proto_library(
    name = "proto_taskscheduleplan",
    srcs = ["taskscheduleplan.proto"],
    proto_source_root = "twister2/proto",
)

native.java_proto_library(
    name = "proto_taskscheduleplan_java",
    deps = [":proto_taskscheduleplan"],
)

#JOB MASTER
native.proto_library(
    name = "proto_jobmaster",
    srcs = ["jobmaster.proto"],
    deps = [":proto_job"],
    proto_source_root = "twister2/proto",
)

native.java_proto_library(
    name = "proto_jobmaster_java",
    deps = [":proto_jobmaster"],
)

#CHECKPOINT
native.proto_library(
    name = "proto_checkpoint",
    srcs = ["checkpoint.proto"],
    proto_source_root = "twister2/proto",
)

native.java_proto_library(
    name = "proto_checkpoint_java",
    deps = [":proto_checkpoint"],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
