package(default_visibility = ["//visibility:public"])

#java_library(
#    name = "taskscheduler-java",
#    srcs = glob(["**/*.java"]),
#    deps = [
#        "//twister2/common/src/java:common-java",
#        "//twister2/common/src/java:config-java",
#        "//twister2/proto:proto-taskscheduleplan-java",
#        "//twister2/proto:proto_taskscheduleplan_java",
#        "@com_google_guava_guava//jar",
#        "@com_google_protobuf_protobuf_java//jar",
#        "@commons_cli_commons_cli//jar",
#        "@commons_io_commons_io//jar",
#    ],
#)

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/common/src/java:common-java",
    "//twister2/task/src/main/java:task-java",
    "//twister2/proto:proto-taskscheduleplan-java",
    "//twister2/proto:proto_taskscheduleplan_java",
    "//twister2/api/src/java:api-java",
    "//twister2/data/src/main/java:data-java",
    "//twister2/proto:proto_job_java",
    "@com_google_guava_guava//jar",
    "@lmdb_java//jar",
    "@commons_lang_commons_lang//jar",
    "@commons_cli_commons_cli//jar",
    "@com_google_protobuf_protobuf_java//jar",
    "@commons_io_commons_io//jar",
]

java_library(
    name = "taskscheduler-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)

#package(default_visibility = ["//visibility:public"])

#java_library(
#    name = "taskscheduler-java",
#    srcs = glob(["**/*.java"]),
#    deps = [
#        "//twister2/api/src/java:api-java",
#        "//twister2/common/src/java:common-java",
#        "//twister2/comms/src/java:comms-java",
#        "//twister2/data/src/main/java:data-java",
#        "//twister2/proto:proto-taskscheduleplan-java",
#        "//twister2/proto:proto_job_java",
#        "//twister2/proto:proto_taskscheduleplan_java",
#        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
#        "//twister2/task/src/main/java:task-java",
#        "@com_google_guava_guava//jar",
#        "@commons_cli_commons_cli//jar",
#        "@commons_lang_commons_lang//jar",
#        "@lmdb_java//jar",
#        "@org_yaml_snakeyaml//jar",
#    ],
#)

#java_library(
#    name = "taskscheduler-java",
#    srcs = glob(["**/*.java"]),
#    deps = common_deps_files,
#)
