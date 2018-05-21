package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "//twister2/task/src/main/java:task-java",
    "//twister2/data/src/main/java:data-java",
    "//twister2/executor/src/java:executor-java",
    "//twister2/proto:proto_job_state_java"
]

java_library(
    name = "master-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)
