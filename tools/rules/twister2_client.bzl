# Twister2 client files

def twister2_client_bin_files():
    return [
        "//twister2/tools/cli/src/python:twister2",
    ]

def twister2_client_conf_files():
    return [
        "//twister2/config/src/yaml:conf-yaml",
    ]

def twister2_client_local_files():
    return [
        "//twister2/config/src/yaml:conf-local-yaml",
    ]

def twister2_client_lib_task_scheduler_files():
    return [
        "//twister2/taskscheduler/src/java:task_scheduler-java",
    ]

def twister2_client_lib_resource_scheduler_files():
    return [
        "//twister2/resource-scheduler/src/java:resource-scheduler-java",
    ]

def twister2_client_lib_api_files():
    return [
        "//twister2/api/src/java:api-java",
    ]

def twister2_client_lib_third_party_files():
    return [
        "@com_google_protobuf_protobuf_java//jar",
        "@org_slf4j_slf4j_api//jar",
        "@org_slf4j_slf4j_jdk14//jar",
    ]
