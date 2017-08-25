package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//third_party:ompi_javabinding_java",
    "//twister2/common/src/java:config-java",
]

java_library(
    name = "comms-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)

