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
        "@org_glassfish_hk2_hk2_locator//jar",
        "@org_glassfish_hk2_hk2_api//jar",
        "@org_javassist_javassist//jar",
        "@org_glassfish_hk2_osgi_resource_locator//jar",
        "@org_glassfish_hk2_hk2_utils//jar",
        "@org_glassfish_hk2_external_aopalliance_repackaged//jar",
        "@org_glassfish_jersey_bundles_repackaged_jersey_guava//jar",
        "@org_glassfish_jersey_inject_jersey_hk2//jar",
        "@javax_annotation_javax_annotation_api//jar",
    ]
