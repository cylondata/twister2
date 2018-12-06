package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "taskscheduler-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Task Scheduler",
    deps = [
        "@org_yaml_snakeyaml//jar",
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/common/src/java:common-java",
        "//twister2/task/src/main/java:task-java",
        "//twister2/proto:proto-java",
        #"//twister2/api/src/java:api-java",
        "//twister2/data/src/main/java:data-java",
        "@com_google_guava_guava//jar",
        "@org_lmdbjava_lmdbjava//jar",
        "@org_apache_commons_commons_lang3//jar",
        "@commons_cli_commons_cli//jar",
        "@com_google_protobuf//:protobuf_java",
        "@commons_io_commons_io//jar",
        "@org_apache_hadoop_hadoop_hdfs//jar",
        "@org_apache_hadoop_hadoop_common//jar",
        "@org_apache_hadoop_hadoop_annotations//jar",
        "@org_apache_hadoop_hadoop_auth//jar",
        "@org_apache_hadoop_hadoop_mapreduce_client_core//jar",
        "@com_google_code_findbugs_jsr305//jar",
        "@com_fasterxml_woodstox_woodstox_core//jar",
        "@org_codehaus_woodstox_stax2_api//jar",
        "@commons_collections_commons_collections//jar",
        "@commons_configuration_commons_configuration//jar",
        "@log4j_log4j//jar",
        "@org_apache_htrace_htrace_core4//jar",
        "@org_apache_hadoop_hadoop_hdfs_client//jar",
    ],
)
