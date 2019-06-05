load("//tools:rules/java_tests.bzl", "java_tests")

java_library(
    name = "task-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//third_party:ompi_javabinding_java",
        "//third_party/java:junit4",
        "//third_party/java:mockito",
        "//third_party/java:powermock",
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/data/src/main/java:data-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_guava_guava//jar",
        "@org_apache_commons_commons_lang3//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)

java_tests(
    test_classes = [
        "edu.iu.dsc.tws.task.test.Hello",
    ],
    runtime_deps = [
        ":task-tests",
    ],
)

#java_tests_debug(
#    size = "enormous",
#    test_classes = [
#        "edu.iu.dsc.tws.comms.shuffle.FSMergerTest",
#        "edu.iu.dsc.tws.comms.shuffle.FSKeyedMergerTest",
#        "edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMergerTest",
#        "edu.iu.dsc.tws.comms.utils.HeapTest",
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
