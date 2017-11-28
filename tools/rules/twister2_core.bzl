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
    ]
