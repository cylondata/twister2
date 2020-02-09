package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "python-support",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Python Support",
    deps = [
        "//third_party:ompi_javabinding_java",
        "//twister2/api/src/java:api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/tset:tset-api-java",
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
        "//twister2/tools/local-runner/src/java:local-runner-java",
        "//twister2/tset/src/java:tset-java",
        "@black_ninia_jep//jar",
        "@net_sf_py4j_py4j//jar",
    ],
)
