package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib", "t2_proto_java_lib")

#JOB STATE
#native.proto_library(
#    name = "proto_job_state",
#    srcs = ["job_state.proto"],
#    proto_source_root = "twister2/proto",
#)

#native.java_proto_library(
#    name = "proto_job_state_java",
#    deps = [":proto_job_state"],
#)

#JOB
#native.proto_library(
#    name = "proto_job",
#    srcs = ["job.proto"],
#    proto_source_root = "twister2/proto",
#)

#native.java_proto_library(
#    name = "proto_job_java",
#    deps = [":proto_job"],
#)

#TASK SCHEDULE PLAN
#native.proto_library(
#    name = "proto_taskscheduleplan",
#    srcs = ["taskscheduleplan.proto"],
#    proto_source_root = "twister2/proto",
#)

#native.java_proto_library(
#    name = "proto_taskscheduleplan_java",
#    deps = [":proto_taskscheduleplan"],
#)

#JOB MASTER
#native.proto_library(
#    name = "proto_jobmaster",
#    srcs = ["jobmaster.proto"],
#    deps = [":proto_job"],
#    proto_source_root = "twister2/proto",
#)

#native.java_proto_library(
#    name = "proto_jobmaster_java",
#    deps = [":proto_jobmaster"],
#)

#CHECKPOINT
#native.proto_library(
#    name = "proto_checkpoint",
#    srcs = ["checkpoint.proto"],
#    proto_source_root = "twister2/proto",
#)

#native.java_proto_library(
#    name = "proto_checkpoint_java",
#    deps = [":proto_checkpoint"],
#)

#UNIFIED JAR
native.proto_library(
    name = "proto",
    srcs = [
        "checkpoint.proto",
        "job.proto",
        "job_state.proto",
        "jobmaster.proto",
        "taskscheduleplan.proto",
    ],
    proto_source_root = "twister2/proto",
)

t2_proto_java_lib(
    name = "proto-java",
    artifact_name = "Twister2 Proto",
    deps = [
        ":proto",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
