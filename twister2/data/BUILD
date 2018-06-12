package(default_visibility = ["//visibility:public"])

common_deps_files = [
    "@org_yaml_snakeyaml//jar",
    "//twister2/common/src/java:config-java",
    "//twister2/common/src/java:common-java",
    "@com_esotericsoftware_kryo//jar",
    "//third_party:com_github_jnr_jffi_native",
    "@com_google_guava_guava//jar",
    "@lmdb_java//jar",
    "@lmdbjava_native_linux//jar",
    "@lmdbjava_native_windows//jar",
    "@lmdbjava_native_osx//jar",
    "@com_github_jnr_ffi//jar",
    "@com_github_jnr_constants//jar",
    "@commons_lang_commons_lang//jar",
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

filegroup(
    name = "data-srcs",
    srcs = glob(["**"]) + [
        "//twister2/data/src/main/java:srcs",
        "//twister2/data/src/test/java:srcs",
    ],
)
