package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "@com_google_protobuf_protobuf_java//jar",
    "@com_google_guava_guava//jar",
    "//twister2/proto:proto_job_java",
    "//twister2/proto:proto_jobmaster_java",
]

t2_java_lib(
    name = "common-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Common",
    deps = common_deps_files,
)

t2_java_lib(
    name = "config-java",
    srcs = glob(["**/config/*.java"]),
    artifact_name = "Twister2 Common Configuration",
    generate_pom = False,
    deps = [
        "@com_google_protobuf_protobuf_java//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)
