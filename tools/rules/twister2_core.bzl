# Utility macros for Twister2 core files

def twister2_core_files():
    return twister2_core_conf_files() + twister2_core_lib_files()

def twister2_core_conf_files():
    return [
        "//twister2/config/src/yaml:config-system-yaml",
    ]

def twister2_core_lib_files():
    return twister2_core_lib_resource_scheduler_files() + \
        twister2_core_lib_task_scheduler_files() + \
        twister2_core_lib_communication_files()

def twister2_core_lib_resource_scheduler_files():
    return [
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    ]

def twister2_core_lib_task_scheduler_files():
    return [
        "//twister2/taskscheduler/src/java:taskscheduler-java",
    ]

def twister2_core_lib_communication_files():
    return [
        "//twister2/comms/src/java:comms-java",
        "//twister2/proto:proto-jobmaster-java",
    ]

def twister2_core_lib_connector_files():
      return [
          "//twister2/connectors/src/java:connector-java",
          "@org_xerial_snappy_snappy_java//jar",
          "@org_lz4_lz4_java//jar",
          "@org_slf4j_slf4j_api//jar",
          "@org_apache_kafka_kafka_clients//jar",
      ]

def twister2_client_lib_master_files():
      return [
          "//twister2/connectors/src/java:master-java"
      ]

def twister2_core_lib_data_files():
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

def twister2_core_lib_executor_files():
    return [
        "//twister2/executor/src/java:executor-java",
    ]

def twister2_core_lib_data_lmdb_files():
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