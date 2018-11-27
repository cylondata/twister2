package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/common/src/java:common-java",
    "@com_esotericsoftware_kryo//jar",
    "//third_party:com_github_jnr_jffi_native",
    "@com_google_guava_guava//jar",
    "@org_lmdbjava_lmdbjava//jar",
    "@org_lmdbjava_lmdbjava_native_linux_x86_64//jar",
    "@org_lmdbjava_lmdbjava_native_windows_x86_64//jar",
    "@org_lmdbjava_lmdbjava_native_osx_x86_64//jar",
    "@com_github_jnr_jnr_ffi//jar",
    "@com_github_jnr_jnr_constants//jar",
    "@org_apache_commons_commons_lang3//jar",
    "@org_apache_hadoop_hadoop_hdfs//jar",
    "@org_apache_hadoop_hadoop_common//jar",
    "@org_apache_hadoop_hadoop_annotations//jar",
    "@org_apache_hadoop_hadoop_auth//jar",
    "@org_apache_hadoop_hadoop_mapreduce_client_core//jar",
    "@com_google_code_findbugs_jsr305//jar",
    "@com_fasterxml_woodstox_woodstox_core//jar",
    "@org_codehaus_woodstox_stax2_api//jar",
    "@commons_io_commons_io//jar",
    "@commons_collections_commons_collections//jar",
    "@org_apache_commons_commons_lang3//jar",
    "@commons_configuration_commons_configuration//jar",
    "@log4j_log4j//jar",
    "@org_apache_htrace_htrace_core4//jar",
    "@org_apache_hadoop_hadoop_hdfs_client//jar",
]

filegroup(
    name = "data-srcs",
    srcs = glob(["**"]) + [
        "//twister2/data/src/main/java:srcs",
        "//twister2/data/src/test/java:srcs",
    ],
)
