package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib", "t2_proto_java_lib")
#load("@rules_proto//proto:defs.bzl", "proto_lang_toolchain", "proto_library")
load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_java//java:defs.bzl", "java_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

#UNIFIED JAR
cc_proto_library(
    name = "proto-cpp",
    deps = [":proto"]
)

proto_library(
    name = "proto",
    deps = [
      ":taskscheduleplan-proto",
      ":htgjob-proto",
      ":jobmaster-proto",
      ":checkpoint-proto",
      ":job-state-proto",
      ":job-proto"
    ]
)

proto_library(
  name = "taskscheduleplan-proto",
  srcs = ["taskscheduleplan.proto"]
)

proto_library(
  name = "htgjob-proto",
  srcs = ["htgjob.proto"],
  deps= [":job-proto",":jobmaster-proto"]
)

proto_library(
  name = "jobmaster-proto",
  srcs = ["jobmaster.proto"],
  deps= [":job-proto"]
)

proto_library(
  name = "checkpoint-proto",
  srcs = ["checkpoint.proto"]
)

proto_library(
  name = "job-state-proto",
  srcs = ["job_state.proto"]
)

proto_library(
  name = "job-proto",
  srcs = ["job.proto"]
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
