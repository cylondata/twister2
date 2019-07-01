package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "@com_google_protobuf_protobuf_java//jar",
    "@com_google_guava_guava//jar",
    "@com_esotericsoftware_kryo//jar",
    "//twister2/proto:proto-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/net:network-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/util:api-utils-java",
    "//third_party:ompi_javabinding_java",
]

t2_java_lib(
    name = "common-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Common",
    deps = common_deps_files,
)

t2_java_lib(
    name = "config-java",
    srcs = glob([
        "**/config/*.java",
        "**/kryo/*.java",
    ]),
    artifact_name = "Twister2 Common Configuration",
    generate_pom = False,
    deps = [
        "//twister2/api/src/java:api-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)
