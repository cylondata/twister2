package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "@com_esotericsoftware_kryo//jar",
    "@com_google_guava_guava//jar",
    "@commons_lang_commons_lang//jar",
    "//third_party:ompi_javabinding_java",
    "//twister2/common/src/java:config-java",
    "//twister2/common/src/java:common-java",
    "//twister2/comms/src/java:comms-java",
    "//twister2/data/src/main/java:data-java",
]

java_library(
    name = "task-java",
    srcs = glob(["**/*.java"]),
    # srcs = ["edu/iu/dsc/tws/task/api/Queue.java"],
    deps = common_deps_files,
)

filegroup(
    name = "srcs",
    srcs = ["BUILD"] + glob(["**/*.java"]),
)
