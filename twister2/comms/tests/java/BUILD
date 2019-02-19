load("//tools:rules/java_tests.bzl", "java_tests")

java_library(
    name = "comms-tests",
    srcs = glob(["**/*.java"]),
    deps = [
           "@org_yaml_snakeyaml//jar",
           "@com_esotericsoftware_kryo//jar",
           "@com_google_guava_guava//jar",
           "@org_apache_commons_commons_lang3//jar",
           "@org_objenesis_objenesis//jar",
           "@com_esotericsoftware_minlog//jar",
           "@com_esotericsoftware_reflectasm//jar",
           "@org_ow2_asm_asm//jar",
           "//third_party:ompi_javabinding_java",
           "//twister2/common/src/java:common-java",
           "//twister2/data/src/main/java:data-java",
           "@net_openhft_chronicle_queue//jar",
           "@commons_io_commons_io//jar",
           "@com_google_protobuf//:protobuf_java",
           "//third_party/java:mockito",
           "//third_party/java:junit4",
           "//third_party/java:powermock",
           "//twister2/comms/src/java:comms-java",
           ],
)

java_tests(
    test_classes = [
        "edu.iu.dsc.tws.comms.shuffle.FSMergerTest",
        "edu.iu.dsc.tws.comms.shuffle.FSKeyedMergerTest",
        "edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMergerTest",
        "edu.iu.dsc.tws.comms.utils.HeapTest",
        "edu.iu.dsc.tws.comms.dfw.io.UnifiedSerializerTest",
        "edu.iu.dsc.tws.comms.dfw.io.KeyedUnifiedSerializerTest",
        "edu.iu.dsc.tws.comms.routing.BinaryTreeTest",
    ],
    runtime_deps = [
        ":comms-tests",
    ],
)

java_tests_debug(
    test_classes = [
        #"edu.iu.dsc.tws.comms.shuffle.FSMergerTest",
        #"edu.iu.dsc.tws.comms.shuffle.FSKeyedMergerTest",
        #"edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMergerTest",
        #"edu.iu.dsc.tws.comms.utils.HeapTest",
        "edu.iu.dsc.tws.comms.dfw.io.UnifiedSerializerTest",
        "edu.iu.dsc.tws.comms.dfw.io.KeyedUnifiedSerializerTest",
        "edu.iu.dsc.tws.comms.routing.BinaryTreeTest",
    ],
    runtime_deps = [
        ":comms-tests",
    ],
    size = "enormous",
)
