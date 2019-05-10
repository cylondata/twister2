package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "test-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 Testbed",
    deps = [
        "@com_fasterxml_jackson_core_jackson_core",
        "@com_fasterxml_jackson_core_jackson_databind",
    ],
)

filegroup(
    name = "test",
    srcs = glob(["*.py"]),
)

filegroup(
    name = "test_values",
    srcs = glob(["values/*.py"]),
)

filegroup(
    name = "test_util",
    srcs = glob(["util/*.py"]),
)

filegroup(
    name = "test_tree",
    srcs = glob(["tree/*.py"]),
)

filegroup(
    name = "test_tests",
    srcs = glob(["tests/*.json"]),
)

filegroup(
    name = "test_tests_apps_terasort",
    srcs = glob(["tests/apps/**/*.json"]),
)

filegroup(
    name = "test_tests_comms",
    srcs = glob(["tests/comms/*.json"]),
)

filegroup(
    name = "test_tests_integration",
    srcs = glob(["tests/integration/*.json"]),
)

filegroup(
    name = "test_tests_integration_comms",
    srcs = glob(["tests/integration/comms/*.json"]),
)


filegroup(
    name = "test_tests_integration_task",
    srcs = glob(["tests/integration/task/*.json"]),
)
