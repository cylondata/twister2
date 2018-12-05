package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "master-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Master",
    deps = [
        "//twister2/common/src/java:common-java",
        "//twister2/data/src/main/java:data-java",
        "//twister2/proto:proto_checkpoint_java",
        "//twister2/proto:proto_job_state_java",
        "//twister2/proto:proto_jobmaster_java",
        "//twister2/task/src/main/java:task-java",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_base//jar",
        "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider//jar",
        "@com_google_protobuf//:protobuf_java",
        "@javax_ws_rs_javax_ws_rs_api//jar",
        "@org_glassfish_hk2_external_javax_inject//jar",
        "@org_glassfish_jersey_core_jersey_client//jar",
        "@org_glassfish_jersey_core_jersey_common//jar",
    ],
)
