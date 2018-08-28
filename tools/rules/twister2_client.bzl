# Twister2 client files

def twister2_client_bin_files():
    return [
        "//twister2/tools/cli/src/python:twister2",
        "//third_party/nomad:nomad",
    ]

def twister2_client_conf_files():
    return [
        "//twister2/config/src/yaml:conf-yaml",
        "//twister2/config/src/yaml:conf-local-yaml",
        "//twister2/config/src/yaml:conf-slurmmpi-yaml",
        "//twister2/config/src/yaml:conf-nodesmpi-yaml",
        "//twister2/config/src/yaml:conf-aurora-yaml",
        "//twister2/config/src/yaml:conf-kubernetes-yaml",
	      "//twister2/config/src/yaml:conf-mesos-yaml",
    ]

def twister2_client_local_files():
    return [
        "//twister2/config/src/yaml:conf-local-yaml",
    ]

def twister2_client_nodesmpi_files():
    return [
        "//twister2/config/src/yaml:conf-nodesmpi-yaml",
    ]

def twister2_client_slurmmpi_files():
    return [
        "//twister2/config/src/yaml:conf-slurmmpi-yaml",
    ]

def twister2_client_aurora_files():
    return [
        "//twister2/config/src/yaml:conf-aurora-yaml",
    ]

def twister2_client_kubernetes_files():
    return [
        "//twister2/config/src/yaml:conf-kubernetes-yaml",
    ]

def twister2_client_mesos_files():
    return [
        "//twister2/config/src/yaml:conf-mesos-yaml",
    ]

def twister2_client_standalone_files():
    return [
        "//twister2/config/src/yaml:conf-standalone-yaml",
    ]

def twister2_client_lib_task_scheduler_files():
    return [
        "//twister2/taskscheduler/src/java:taskscheduler-java",
    ]

def twister2_client_lib_resource_scheduler_files():
    return [
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
        "@commons_cli_commons_cli//jar",
        "//twister2/proto:proto_job_java",
        "//third_party:ompi_javabinding_java",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "//twister2/proto:proto_job_state_java",
        "@commons_io_commons_io//jar",
        "@org_apache_commons_commons_compress//jar",
        "@org_apache_curator_curator_client//jar",
        "@org_apache_curator_curator_framework//jar",
        "@org_apache_curator_curator_recipes//jar",
        "@org_apache_zookeeper_zookeeper//jar",
        "@io_kubernetes_client_java//jar",
        "@io_kubernetes_client_java_api//jar",
        "@io_kubernetes_client_java_proto//jar",
        "@io_swagger_swagger_annotations//jar",
        "@com_google_code_gson_gson//jar",
        "@com_squareup_okhttp_okhttp//jar",
        "@com_squareup_okhttp_logging_interceptor//jar",
        "@com_squareup_okhttp_okhttp_ws//jar",
        "@com_squareup_okio_okio//jar",
        "@joda_time_joda_time//jar",
        "@commons_codec_commons_codec//jar",
        "@com_hashicorp_nomad//jar",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@com_google_code_findbugs_jsr305//jar",
        "@commons_logging_commons_logging//jar",
        "@org_apache_httpcomponents_http_client//jar",
        "@org_apache_httpcomponents_http_core//jar",
        "@org_bouncycastle_bcpkix_jdk15on//jar",
        "@org_bouncycastle_bcprov_jdk15on//jar",
    ]

def twister2_client_lib_api_files():
    return [
        "//twister2/api/src/java:api-java",
    ]

def twister2_client_lib_task_files():
    return [
        "//twister2/task/src/main/java:task-java",
    ]

def twister2_client_lib_data_files():
    return [
        "//twister2/data/src/main/java:data-java",
        "@org_apache_hadoop_hdfs//jar",
        "@org_apache_hadoop_common//jar",
        "@org_apache_hadoop_annotations//jar",
        "@org_apache_hadoop_auth//jar",
        "@org_apache_hadoop_mapreduce//jar",
        "@com_google_code_findbugs//jar",
        "@com_fasterxml_woodstox//jar",
        "@org_codehaus_woodstox//jar",
        "@commons_io//jar",
        "@commons_collections//jar",
        "@commons_lang//jar",
        "@commons_configuration//jar",
        "@log4j//jar",
        "@org_apache_htrace//jar",
        "@org_apache_hadoop//jar",
    ]
def twister2_client_lib_connector_files():
    return [
        "//twister2/connectors/src/java:connector-java",
        "@org_xerial_snappy_snappy_java//jar",
        "@org_lz4_lz4_java//jar",
        "@org_slf4j_slf4j_api//jar",
        "@org_apache_kafka_kafka_clients//jar",
    ]

def twister2_client_lib_executor_files():
    return [
        "//twister2/executor/src/java:executor-java",
    ]

def twister2_client_lib_data_lmdb_files():
    return [
        "//twister2/data/src/main/java:data-java",
        "@lmdb_java//jar",
        "@lmdbjava_native_linux//jar",
        "@lmdbjava_native_windows//jar",
        "@lmdbjava_native_osx//jar",
        "@com_github_jnr_ffi//jar",
        "@com_github_jnr_constants//jar",
        "@com_github_jnr_jffi//jar",
        "//third_party:com_github_jnr_jffi_native",
    ]

def twister2_client_lib_communication_files():
    return [
        "//twister2/comms/src/java:comms-java",
        "@org_yaml_snakeyaml//jar",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_guava_guava//jar",
        "@commons_lang_commons_lang//jar",
        "@org_objenesis_objenesis//jar",
        "@com_esotericsoftware_minlog//jar",
        "@com_esotericsoftware_reflectasm//jar",
        "@org_ow2_asm_asm//jar",
        "//third_party:ompi_javabinding_java",
    ]

def twister2_client_lib_common_files():
    return [
        "//twister2/common/src/java:config-java",
        "//twister2/common/src/java:common-java",
    ]

def twister2_client_example_files():
    return [
        "//twister2/examples/src/java:examples-java",
    ]

def twister2_client_lib_third_party_files():
    return [
        "@com_google_protobuf_protobuf_java//jar",
        "@org_slf4j_slf4j_api//jar",
        "@org_slf4j_slf4j_jdk14//jar",
    ]

def twister2_client_lib_master_files():
    return [
        "//twister2/connectors/src/java:master-java"
    ]

#def twister2_client_lib_connector_files():
#    return [
#        "//twister2/connectors/src/java:connector-java",
#        "@org_xerial_snappy_snappy_java//jar",
#        "@org_lz4_lz4_java//jar",
#        "@org_slf4j_slf4j_api//jar",
#        "@org_apache_kafka_kafka_clients//jar",
#        "@org_apache_kafka_kafka_clients//jar",
#    ]
