package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "@com_esotericsoftware_kryo//jar",
    "@com_google_guava_guava//jar",
    "@commons_lang_commons_lang//jar",
    "@org_objenesis_objenesis//jar",
    "@com_esotericsoftware_minlog//jar",
    "@com_esotericsoftware_reflectasm//jar",
    "@org_ow2_asm_asm//jar",
    "//third_party:ompi_javabinding_java",
    "//twister2/common/src/java:config-java",
    "//twister2/common/src/java:common-java",
    "//twister2/data/src/main/java:data-java",
    "@net_openhft_chronicle_queue//jar",
    "@commons_io//jar",
]

java_library(
    name = "comms-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)

java_binary(
    name = "comms-java-bin",
    srcs = glob(["**/*.java"]),
    jvm_flags = ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"],
    main_class = "edu.iu.dsc.tws.comms.mpi.io.Test",
    deps = common_deps_files,
)

java_binary(
    name = "comms-java-master",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)

java_binary(
    name = "comms-java-channel-1",
    srcs = glob(["**/*.java"]),
    jvm_flags = ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 -Djava.util.logging.config.file=twister2/config/src/yaml/conf/nodesmpi/logger.properties"],
    main_class = "edu.iu.dsc.tws.comms.tcp.net.TCPChannel",
    deps = common_deps_files,
)

java_binary(
    name = "comms-java-channel-2",
    srcs = glob(["**/*.java"]),
    jvm_flags = ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007 -Djava.util.logging.config.file=twister2/config/src/yaml/conf/nodesmpi/logger.properties"],
    main_class = "edu.iu.dsc.tws.comms.tcp.net.TCPChannel",
    deps = common_deps_files,
)
