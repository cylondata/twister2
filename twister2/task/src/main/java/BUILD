package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "task-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Task API",
    deps = [
        "//third_party:ompi_javabinding_java",
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/data/src/main/java:data-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_guava_guava//jar",
        "@commons_lang_commons_lang//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)

filegroup(
    name = "srcs",
    srcs = ["BUILD"] + glob(["**/*.java"]),
)
