load("//tools/rules:java_tests.bzl", "java_tests")

srcs_without_ucx = glob(["**/*.java"])

srcs_without_ucx.remove("edu/iu/dsc/tws/comms/ucx/TWSUCXChannelTest.java")

java_library(
    name = "comms-tests",
    srcs = select({
        "@bazel_tools//src/conditions:darwin": srcs_without_ucx,
        "//conditions:default": glob(["**/*.java"]),
    }),
    deps = [
        "//third_party:ompi_javabinding_java",
        "//third_party/java:junit4",
        "//third_party/java:mockito",
        "//third_party/java:powermock",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/util:api-utils-java",
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/proto:jobmaster-proto",
        "@com_esotericsoftware_kryo//jar",
        "@com_esotericsoftware_minlog//jar",
        "@com_esotericsoftware_reflectasm//jar",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf//:protobuf_java",
        "@commons_io_commons_io//jar",
        "@net_openhft_chronicle_queue//jar",
        "@org_apache_commons_commons_lang3//jar",
        "@org_objenesis_objenesis//jar",
        "@org_ow2_asm_asm//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)

java_tests(
    test_classes = [
        "edu.iu.dsc.tws.comms.shuffle.FSMergerTest",
        "edu.iu.dsc.tws.comms.shuffle.FSKeyedMergerTest",
        "edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMergerTest",
        "edu.iu.dsc.tws.comms.shuffle.RestorableIteratorTest",
        "edu.iu.dsc.tws.comms.utils.HeapTest",
        "edu.iu.dsc.tws.comms.utils.SortJoinUtilsTest",
        "edu.iu.dsc.tws.comms.utils.HashJoinUtilsTest",
        "edu.iu.dsc.tws.comms.utils.JoinRandomTest",
        "edu.iu.dsc.tws.comms.dfw.io.DataSerializerTest",
        "edu.iu.dsc.tws.comms.dfw.io.KeyedSerializerTest",
        "edu.iu.dsc.tws.comms.dfw.io.KeyedSerializerLargeTest",
        "edu.iu.dsc.tws.comms.dfw.io.AggregatorListTest",
        "edu.iu.dsc.tws.comms.routing.BinaryTreeTest",
        "edu.iu.dsc.tws.comms.selectors.HashingSelectorTest",
    ],
    runtime_deps = [
        ":comms-tests",
    ],
)

#java_tests(
#    classpath_resources = [
#        "@ucx//:ucx-so-files",
#    ],
#    test_classes = [
#        "edu.iu.dsc.tws.comms.ucx.TWSUCXChannelTest",
#    ],
#    runtime_deps = [
#        ":comms-tests",
#    ],
#)

#java_tests_debug(
#    size = "enormous",
#    test_classes = [
#        "edu.iu.dsc.tws.comms.shuffle.FSMergerTest",
#        "edu.iu.dsc.tws.comms.shuffle.FSKeyedMergerTest",
#        "edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMergerTest",
#        "edu.iu.dsc.tws.comms.shuffle.RestorableIteratorTest",
#        "edu.iu.dsc.tws.comms.ucx.TWSUCXChannelTest",
#        "edu.iu.dsc.tws.comms.utils.HeapTest",
#        "edu.iu.dsc.tws.comms.utils.JoinUtilsTest",
#        "edu.iu.dsc.tws.comms.utils.HashJoinUtilsTest",
#        "edu.iu.dsc.tws.comms.dfw.io.DataSerializerTest",
#        "edu.iu.dsc.tws.comms.dfw.io.KeyedSerializerTest",
#        "edu.iu.dsc.tws.comms.dfw.io.KeyedSerializerLargeTest",
#        "edu.iu.dsc.tws.comms.dfw.io.AggregatorListTest",
#        "edu.iu.dsc.tws.comms.routing.BinaryTreeTest",
#    ],
#    runtime_deps = [
#        ":comms-tests",
#    ],
#)
