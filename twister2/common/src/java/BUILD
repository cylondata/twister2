package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "@com_google_protobuf_protobuf_java//jar",
]

java_library(
    name = "common-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)

java_library(
    name = "config-java",
    srcs = glob(["**/config/*.java"]),
    deps = [
        "@org_yaml_snakeyaml//jar",
        "@com_google_protobuf_protobuf_java//jar",
    ],
)
