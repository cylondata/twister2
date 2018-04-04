package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/common/src/java:config-java",
    "//twister2/common/src/java:common-java",
]

java_library(
    name = "executor-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)


