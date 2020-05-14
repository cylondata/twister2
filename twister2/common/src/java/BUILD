package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
    "@com_google_protobuf_protobuf_java//jar",
    "@com_google_guava_guava//jar",
    "@com_esotericsoftware_kryo//jar",
    "@org_apache_commons_commons_lang3//jar",
    "@maven//:com_google_code_gson_gson",
    "@maven//:org_apache_curator_curator_client",
    "@maven//:org_apache_curator_curator_framework",
    "@maven//:org_apache_curator_curator_recipes",
    "@maven//:org_apache_zookeeper_zookeeper",
    "@org_yaml_snakeyaml//jar",
    "//twister2/proto:proto-java",
    "//twister2/proto/utils:proto-utils-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/faulttolerance:fault-tolerance-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/net:network-api-java",
    "//twister2/api/src/java/edu/iu/dsc/tws/api/util:api-utils-java",
    "//third_party:ompi_javabinding_java",
    "@maven//:org_apache_arrow_arrow_vector",
    "@maven//:org_apache_arrow_arrow_memory",
    "@maven//:org_apache_arrow_arrow_format",
    "@maven//:com_google_flatbuffers_flatbuffers_java",
    "@maven//:io_netty_netty_buffer",
    "@maven//:io_netty_netty_all",
    "@maven//:io_netty_netty_transport",
    "@maven//:io_netty_netty_transport_native_epoll",
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
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/util:api-utils-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)
