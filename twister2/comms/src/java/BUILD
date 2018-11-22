package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "comms-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twsiter2 Communication",
    deps = [
        "//third_party:ompi_javabinding_java",
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_guava_guava//jar",
        "@commons_io_commons_io//jar",
        "@org_apache_commons_commons_lang3//jar",
    ],
)

java_binary(
    name = "comms-java-bin",
    srcs = glob(["**/*.java"]),
    jvm_flags = ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"],
    main_class = "edu.iu.dsc.tws.comms.mpi.io.Test",
    deps = [
        "//third_party:ompi_javabinding_java",
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_guava_guava//jar",
        "@commons_io_commons_io//jar",
        "@org_apache_commons_commons_lang3//jar",
    ],
)

java_binary(
    name = "comms-java-master",
    srcs = glob(["**/*.java"]),
    deps = [
        "//third_party:ompi_javabinding_java",
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_guava_guava//jar",
        "@commons_io_commons_io//jar",
        "@org_apache_commons_commons_lang3//jar",
    ],
)

java_binary(
    name = "comms-java-channel-1",
    srcs = glob(["**/*.java"]),
    jvm_flags = ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 -Djava.util.logging.config.file=twister2/config/src/yaml/conf/standalone/logger.properties"],
    main_class = "edu.iu.dsc.tws.comms.tcp.net.TCPChannel",
    deps = [
        "//third_party:ompi_javabinding_java",
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_esotericsoftware_minlog//jar",
        "@com_esotericsoftware_reflectasm//jar",
        "@com_google_guava_guava//jar",
        "@commons_io_commons_io//jar",
        "@org_apache_commons_commons_lang3//jar",
        "@net_openhft_chronicle_queue//jar",
        "@org_objenesis_objenesis//jar",
        "@org_ow2_asm_asm//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)

java_binary(
    name = "comms-java-channel-2",
    srcs = glob(["**/*.java"]),
    jvm_flags = ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007 -Djava.util.logging.config.file=twister2/config/src/yaml/conf/standalone/logger.properties"],
    main_class = "edu.iu.dsc.tws.comms.tcp.net.TCPChannel",
    deps = [
        "//third_party:ompi_javabinding_java",
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_esotericsoftware_minlog//jar",
        "@com_esotericsoftware_reflectasm//jar",
        "@com_google_guava_guava//jar",
        "@commons_io_commons_io//jar",
        "@org_apache_commons_commons_lang3//jar",
        "@net_openhft_chronicle_queue//jar",
        "@org_objenesis_objenesis//jar",
        "@org_ow2_asm_asm//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)
