package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

common_deps_files = [
       "//twister2/proto:proto-java",
       "//twister2/api/src/java:api-java",
       "//twister2/resource-scheduler/src/java:resource-scheduler-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/compute:task-api-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/checkpointing:checkpointing-api-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/comms:comms-api-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/data:data-api-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/dataset:dataset-api-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/exceptions:exceptions-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/resource:resource-api-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/scheduler:scheduler-api-java",
       "//twister2/api/src/java/edu/iu/dsc/tws/api/tset:tset-api-java",
       "//twister2/tset/src/java:tset-java",
       "@org_apache_beam_beam_runners_core_java//jar",
       "@org_apache_beam_beam_sdks_java_core//jar",
       "@org_apache_beam_beam_model_pipeline//jar",
       "@org_apache_beam_beam_runners_java_fn_execution//jar",
       "@com_fasterxml_jackson_core_jackson_annotations//jar",
       "@joda_time_joda_time//jar",
       "@org_apache_beam_beam_runners_core_construction_java//jar",
       "@com_google_guava_guava//jar",
       "//third_party:vendored_grpc_1_21_0",
       "//third_party:vendored_guava_26_0_jre",
]

t2_java_lib(
    name = "twister2-beam",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twister2 Beam Compatibility",
    deps = common_deps_files,
)
