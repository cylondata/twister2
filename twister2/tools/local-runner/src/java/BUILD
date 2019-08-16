package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "local-runner-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Task Scheduler",
    deps = [
        "//twister2/api/src/java:api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
        "//twister2/executor/src/java:executor-java",
        "//twister2/proto:proto-java",
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    ],
)
