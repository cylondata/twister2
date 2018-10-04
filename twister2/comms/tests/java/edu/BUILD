load("//tools:rules/java_tests.bzl", "java_tests")

comms_deps_files = [
    "//twister2/common/src/java:common-java",
    "//twister2/comms/src/java:comms-java",
    "@org_yaml_snakeyaml//jar",
    "@com_google_protobuf_protobuf_java//jar",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
    "//third_party/java:powermock",
]

java_library(
    name = "comms-tests",
    srcs = glob(["**/*.java"]),
    deps = comms_deps_files,
)

java_tests(
    test_classes = [
        "edu.iu.dsc.tws.comms.shuffle.FSMergerTest",
        "edu.iu.dsc.tws.comms.shuffle.FSKeyedMergerTest",
        "edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMergerTest",
        "edu.iu.dsc.tws.comms.utils.HeapTest",
    ],
    runtime_deps = [
        ":comms-tests",
    ],
)
