# Twister2 client files

def twister2_client_bin_files():
    return [
        "//twister2/tools/cli/src/python:twister2",
    ]

def twister2_client_conf_files():
    return [
        "//twister2/config/src/yaml:conf-yaml",
        "//twister2/config/src/yaml:conf-local-yaml",
        "//twister2/config/src/yaml:conf-slurmmpi-yaml",
    ]

def twister2_client_local_files():
    return [
        "//twister2/config/src/yaml:conf-local-yaml",
    ]

def twister2_client_slurmmpi_files():
    return [
        "//twister2/config/src/yaml:conf-slurmmpi-yaml",
    ]

def twister2_client_lib_task_scheduler_files():
    return [
        "//twister2/taskscheduler/src/java:task_scheduler-java",
    ]

def twister2_client_lib_resource_scheduler_files():
    return [
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
        "@commons_cli_commons_cli//jar",
        "//twister2/proto:proto-resource-scheduler-java",
        "//twister2/proto:proto_job_java",
        "//twister2/proto:proto_resource_scheduler_java",
        "//third_party:ompi_javabinding_java",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "//twister2/proto:proto_job_state_java",
    ]

def twister2_client_lib_api_files():
    return [
        "//twister2/api/src/java:api-java",
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
