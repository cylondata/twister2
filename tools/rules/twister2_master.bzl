# Twister2 master files

def twister2_master_lib_files():
    return [
        "//twister2/master/src/java:master-java",
        "//twister2/proto:proto-java",
    ]

# commented jar files are currently not used.
# we may later need them.
# that is why i keep them here as commented.
def twister2_master_jersey_files():
    return [
        "@com_fasterxml_jackson_module_jackson_module_jaxb_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@javax_annotation_javax_annotation_api//jar",
        "@javax_ws_rs_javax_ws_rs_api//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@org_glassfish_hk2_external_javax_inject//jar",
        "@org_glassfish_hk2_hk2_api//jar",
        "@org_glassfish_hk2_hk2_locator//jar",
        "@org_glassfish_hk2_hk2_utils//jar",
        "@org_glassfish_jersey_core_jersey_client//jar",
        "@org_glassfish_jersey_core_jersey_common//jar",
        "@org_glassfish_jersey_inject_jersey_hk2//jar",
        "@org_glassfish_jersey_media_jersey_media_json_jackson//jar",
        "@org_glassfish_jersey_ext_jersey_entity_filtering//jar",
        # "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_base//jar",
        # "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider//jar",
        # "@org_glassfish_hk2_external_aopalliance_repackaged//jar",
        # "@org_glassfish_hk2_osgi_resource_locator//jar",
        # "@org_glassfish_jersey_bundles_repackaged_jersey_guava//jar",
        # "@org_javassist_javassist//jar",
    ]
