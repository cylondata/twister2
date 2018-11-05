package(default_visibility = ["//visibility:public"])

java_library(
    name = "task-java",
    srcs = glob(["**/*.java"]),
    deps = ["@org_yaml_snakeyaml//jar",
           "@com_esotericsoftware_kryo//jar",
           "@com_google_guava_guava//jar",
           "@commons_lang_commons_lang//jar",
           "//third_party:ompi_javabinding_java",
           "//twister2/common/src/java:common-java",
           "//twister2/comms/src/java:comms-java",
           "//twister2/data/src/main/java:data-java",
           ],
)

filegroup(
    name = "srcs",
    srcs = ["BUILD"] + glob(["**/*.java"]),
)
