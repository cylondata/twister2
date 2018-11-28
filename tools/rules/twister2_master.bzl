# Twister2 master files

def twister2_master_lib_files():
    return [
        "//twister2/master/src/java:master-java",
        "//twister2/proto:proto_jobmaster_java",
    ]

def twister2_master_jersey_files():
    return [
        "@org_glassfish_jersey_core_jersey_client//jar",
        "@org_glassfish_jersey_core_jersey_common//jar",
        "@org_glassfish_hk2_external_javax_inject//jar",
        "@javax_ws_rs_javax_ws_rs_api//jar",
    ]
