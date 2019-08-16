package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "taskscheduler-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Task Scheduler",
    deps = [
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
        "//twister2/data/src/main/java:data-java",
        "@com_fasterxml_woodstox_woodstox_core//jar",
        "@com_google_code_findbugs_jsr305//jar",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf//:protobuf_java",
        "@commons_cli_commons_cli//jar",
        "@commons_collections_commons_collections//jar",
        "@commons_configuration_commons_configuration//jar",
        "@commons_io_commons_io//jar",
        "@log4j_log4j//jar",
        "@org_apache_commons_commons_lang3//jar",
        "@org_apache_hadoop_hadoop_annotations//jar",
        "@org_apache_hadoop_hadoop_auth//jar",
        "@org_apache_hadoop_hadoop_common//jar",
        "@org_apache_hadoop_hadoop_hdfs//jar",
        "@org_apache_hadoop_hadoop_hdfs_client//jar",
        "@org_apache_hadoop_hadoop_mapreduce_client_core//jar",
        "@org_apache_htrace_htrace_core4//jar",
        "@org_codehaus_woodstox_stax2_api//jar",
        "@org_lmdbjava_lmdbjava//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)
