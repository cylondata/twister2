package(default_visibility = ["//visibility:public"])

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar", "pkg_deb")

load("/tools/rules/twister2_client", "twister2_client_bin_files")
load("/tools/rules/twister2_client", "twister2_client_conf_files")
load("/tools/rules/twister2_client", "twister2_client_slurmmpi_files")
load("/tools/rules/twister2_client", "twister2_client_nodesmpi_files")
load("/tools/rules/twister2_client", "twister2_client_aurora_files")
load("/tools/rules/twister2_client", "twister2_client_lib_third_party_files")
load("/tools/rules/twister2_client", "twister2_client_lib_task_scheduler_files")
load("/tools/rules/twister2_client", "twister2_client_lib_resource_scheduler_files")
load("/tools/rules/twister2_client", "twister2_client_lib_api_files")
load("/tools/rules/twister2_client", "twister2_client_lib_task_files")
load("/tools/rules/twister2_client", "twister2_client_lib_data_files")
load("/tools/rules/twister2_client", "twister2_client_lib_data_lmdb_files")
load("/tools/rules/twister2_client", "twister2_client_lib_communication_files")
load("/tools/rules/twister2_client", "twister2_client_example_files")
load("/tools/rules/twister2_client", "twister2_client_lib_common_files")

load("/tools/rules/twister2_core", "twister2_core_files")
load("/tools/rules/twister2_core", "twister2_core_conf_files")
load("/tools/rules/twister2_core", "twister2_core_lib_communication_files")
load("/tools/rules/twister2_core", "twister2_core_lib_resource_scheduler_files")
load("/tools/rules/twister2_core", "twister2_core_lib_task_scheduler_files")

generated_release_files = [
    "release.yaml",
]

filegroup(
    name = "release_files",
    srcs = generated_release_files,
)

filegroup(
    name = "tarpkgs",
    srcs = [
        ":twister2-core",
        ":twister2-client",
    ],
)

# twister2 core packaging

pkg_tar(
    name = "twister2-core",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-core",
    deps = [
        ":twister2-core-lib-resource-scheduler",
        ":twister2-core-lib-task-scheduler",
        ":twister2-core-lib-communication",
        ":twister2-core-lib-api",
        ":twister2-core-lib-task",
        ":twister2-core-lib-common",
        ":twister2-core-lib-third_party",
    ],
)

pkg_tar(
    name = "twister2-core-lib-api",
    package_dir = "lib",
    srcs = twister2_client_lib_api_files(),
)

pkg_tar(
    name = "twister2-core-lib-task",
    package_dir = "lib",
    srcs = twister2_client_lib_task_files(),
)

pkg_tar(
    name = "twister2-core-lib-resource-scheduler",
    package_dir = "lib",
    srcs = twister2_client_lib_resource_scheduler_files(),
)

pkg_tar(
    name = "twister2-core-lib-task-scheduler",
    package_dir = "lib",
    srcs = twister2_client_lib_task_scheduler_files(),
)

pkg_tar(
    name = "twister2-core-lib-third_party",
    package_dir = "lib",
    srcs = twister2_client_lib_third_party_files(),
)

pkg_tar(
    name = "twister2-core-lib-communication",
    package_dir = "lib",
    srcs = twister2_client_lib_communication_files(),
)

pkg_tar(
    name = "twister2-core-lib-common",
    package_dir = "lib",
    srcs = twister2_client_lib_common_files(),
)

# twister2 client packaging

pkg_tar(
    name = "twister2-client",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-dist",
    deps = [
        ":twister2-client-bin",
        ":twister2-client-conf-slurmmpi",
        ":twister2-client-conf-nodesmpi",
        ":twister2-client-conf-aurora",
        ":twister2-client-dist",
        ":twister2-client-lib-third_party",
        ":twister2-client-lib-resource-scheduler",
        ":twister2-client-lib-task-scheduler",
        ":twister2-client-lib-api",
        ":twister2-client-lib-task",
        ":twister2-client-communication",
        ":twister2-client-examples",
        ":twister2-client-lib-common",
        ":twister2-client-lib-data-lmdb",

    ],
)

pkg_tar(
    name = "twister2-client-conf-slurmmpi",
    package_dir = "conf/slurmmpi",
    srcs = twister2_client_slurmmpi_files(),
)

pkg_tar(
    name = "twister2-client-conf-nodesmpi",
    package_dir = "conf/nodesmpi",
    srcs = twister2_client_nodesmpi_files(),
)

pkg_tar(
    name = "twister2-client-conf-aurora",
    package_dir = "conf/aurora",
    srcs = twister2_client_aurora_files(),
)

pkg_tar(
    name = "twister2-client-dist",
    package_dir = "dist",
    srcs = [":twister2-core"],
)

pkg_tar(
    name = "twister2-client-bin",
    package_dir = "bin",
    srcs = twister2_client_bin_files(),
)

pkg_tar(
    name = "twister2-client-lib-api",
    package_dir = "lib",
    srcs = twister2_client_lib_api_files(),
)

pkg_tar(
    name = "twister2-client-lib-task",
    package_dir = "lib",
    srcs = twister2_client_lib_task_files(),
)

pkg_tar(
    name = "twister2-client-lib-data",
    package_dir = "lib",
    srcs = twister2_client_lib_data_files(),
)

pkg_tar(
    name = "twister2-client-lib-data-lmdb",
    package_dir = "lib",
    srcs = twister2_client_lib_data_lmdb_files(),
)


pkg_tar(
    name = "twister2-client-lib-resource-scheduler",
    package_dir = "lib",
    srcs = twister2_client_lib_resource_scheduler_files(),
)

pkg_tar(
    name = "twister2-client-lib-task-scheduler",
    package_dir = "lib",
    srcs = twister2_client_lib_task_scheduler_files(),
)

pkg_tar(
    name = "twister2-client-lib-third_party",
    package_dir = "lib",
    srcs = twister2_client_lib_third_party_files(),
)

pkg_tar(
    name = "twister2-client-communication",
    package_dir = "lib",
    srcs = twister2_client_lib_communication_files(),
)

pkg_tar(
    name = "twister2-client-lib-common",
    package_dir = "lib",
    srcs = twister2_client_lib_common_files(),
)

pkg_tar(
    name = "twister2-client-examples",
    package_dir = "examples",
    srcs = twister2_client_example_files(),
)


