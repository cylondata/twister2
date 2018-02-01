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
]

java_library(
    name = "data-java",
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)

filegroup(
    name = "srcs",
    srcs = ["BUILD"] + glob(["**/*.java"]),
)
