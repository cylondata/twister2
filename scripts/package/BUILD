package(default_visibility = ["//visibility:public"])

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar", "pkg_deb")
load("//tools:rules/twister2_client.bzl", "twister2_client_bin_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_conf_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_slurmmpi_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_nodesmpi_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_aurora_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_kubernetes_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_mesos_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_standalone_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_third_party_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_task_scheduler_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_resource_scheduler_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_api_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_task_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_data_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_data_lmdb_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_communication_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_example_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_common_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_executor_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_lib_connector_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_conf_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_lib_communication_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_lib_resource_scheduler_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_lib_task_scheduler_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_lib_connector_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_lib_data_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_lib_executor_files")
load("//tools:rules/twister2_core.bzl", "twister2_core_lib_data_lmdb_files")
load("//tools:rules/twister2_master.bzl", "twister2_master_lib_files")

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
        ":twister2-client-mpi",
        ":twister2-core-mpi",
    ],
)

filegroup(
    name = "tarpkgs_no_mpi",
    srcs = [
        ":twister2-client",
        ":twister2-core",
    ],
)

######################################################
# twister2 core packaging
######################################################

pkg_tar(
    name = "twister2-core",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-core",
    deps = [
        ":twister2-core-lib-api",
        ":twister2-core-lib-common",
        ":twister2-core-lib-communication",
        ":twister2-core-lib-connector",
        ":twister2-core-lib-data",
        ":twister2-core-lib-lmdb",
        ":twister2-core-lib-resource-scheduler",
        ":twister2-core-lib-task",
        ":twister2-core-lib-task-scheduler",
        ":twister2-core-lib-third_party",
        ":twister2-master-lib",
    ],
)

pkg_tar(
    name = "twister2-core-mpi",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-core",
    deps = [
        ":twister2-core-lib-api",
        ":twister2-core-lib-common",
        ":twister2-core-lib-communication",
        ":twister2-core-lib-connector",
        ":twister2-core-lib-data",
        ":twister2-core-lib-lmdb",
        ":twister2-core-lib-resource-scheduler",
        ":twister2-core-lib-task",
        ":twister2-core-lib-task-scheduler",
        ":twister2-core-lib-third_party",
        ":twister2-master-lib",
        ":twister2-ompi-lib",
        ":twister2-ompi-bin",
        ":twister2-ompi-include",
        ":twister2-ompi-etc",
    ],
)

pkg_tar(
    name = "twister2-ompi-lib",
    srcs = ["@ompi3//:ompi-lib-files",],
    package_dir = "ompi/lib",
)

pkg_tar(
    name = "twister2-ompi-bin",
    srcs = ["@ompi3//:ompi-bin-files",],
    package_dir = "ompi/bin",
)

pkg_tar(
    name = "twister2-ompi-include",
    srcs = ["@ompi3//:ompi-include-files",],
    package_dir = "ompi/include",
)

pkg_tar(
    name = "twister2-ompi-etc",
    srcs = ["@ompi3//:ompi-etc-files",],
    package_dir = "ompi/etc",
)

pkg_tar(
    name = "twister2-core-lib-api",
    srcs = twister2_client_lib_api_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-task",
    srcs = twister2_client_lib_task_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-resource-scheduler",
    srcs = twister2_client_lib_resource_scheduler_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-task-scheduler",
    srcs = twister2_client_lib_task_scheduler_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-third_party",
    srcs = twister2_client_lib_third_party_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-communication",
    srcs = twister2_client_lib_communication_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-common",
    srcs = twister2_client_lib_common_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-executor",
    srcs = twister2_client_lib_executor_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-connector",
    srcs = twister2_client_lib_connector_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-data",
    srcs = twister2_client_lib_data_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-core-lib-lmdb",
    srcs = twister2_core_lib_data_lmdb_files(),
    package_dir = "lib",
)

######################################################
# twister2 client packaging
######################################################

pkg_tar(
    name = "twister2-client",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-dist",
    deps = [
        ":twister2-client-bin",
        ":twister2-client-communication",
        ":twister2-client-conf-aurora",
        ":twister2-client-conf-kubernetes",
        ":twister2-client-conf-mesos",
        ":twister2-client-conf-nodesmpi",
        ":twister2-client-conf-slurmmpi",
        ":twister2-client-conf-standalone",
        ":twister2-client-dist",
        ":twister2-client-examples",
        ":twister2-client-lib-api",
        ":twister2-client-lib-common",
        ":twister2-client-lib-connector",
        ":twister2-client-lib-data",
        ":twister2-client-lib-data-lmdb",
        ":twister2-client-lib-resource-scheduler",
        ":twister2-client-lib-task",
        ":twister2-client-lib-task-scheduler",
        ":twister2-client-lib-third_party",
        ":twister2-core-lib-executor",
        ":twister2-master-lib",
    ],
)

pkg_tar(
    name = "twister2-client-mpi",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-dist",
    deps = [
        ":twister2-client-bin",
        ":twister2-client-communication",
        ":twister2-client-conf-aurora",
        ":twister2-client-conf-kubernetes",
        ":twister2-client-conf-mesos",
        ":twister2-client-conf-nodesmpi",
        ":twister2-client-conf-slurmmpi",
        ":twister2-client-conf-standalone",
        ":twister2-client-dist",
        ":twister2-client-examples",
        ":twister2-client-lib-api",
        ":twister2-client-lib-common",
        ":twister2-client-lib-connector",
        ":twister2-client-lib-data",
        ":twister2-client-lib-data-lmdb",
        ":twister2-client-lib-resource-scheduler",
        ":twister2-client-lib-task",
        ":twister2-client-lib-task-scheduler",
        ":twister2-client-lib-third_party",
        ":twister2-core-lib-executor",
        ":twister2-master-lib",
        ":twister2-ompi-lib",
        ":twister2-ompi-bin",
        ":twister2-ompi-include",
        ":twister2-ompi-etc",
    ],
)

pkg_tar(
    name = "twister2-client-conf-slurmmpi",
    srcs = twister2_client_slurmmpi_files(),
    package_dir = "conf/slurmmpi",
)

pkg_tar(
    name = "twister2-client-conf-nodesmpi",
    srcs = twister2_client_nodesmpi_files(),
    package_dir = "conf/nodesmpi",
)

pkg_tar(
    name = "twister2-client-conf-aurora",
    srcs = twister2_client_aurora_files(),
    package_dir = "conf/aurora",
)

pkg_tar(
    name = "twister2-client-conf-kubernetes",
    srcs = twister2_client_kubernetes_files(),
    package_dir = "conf/kubernetes",
)

pkg_tar(
    name = "twister2-client-conf-mesos",
    srcs = twister2_client_mesos_files(),
    package_dir = "conf/mesos",
)

pkg_tar(
    name = "twister2-client-conf-standalone",
    srcs = twister2_client_standalone_files(),
    package_dir = "conf/standalone",
)

pkg_tar(
    name = "twister2-client-dist",
    srcs = [":twister2-core"],
    package_dir = "dist",
)

pkg_tar(
    name = "twister2-client-bin",
    srcs = twister2_client_bin_files(),
    package_dir = "bin",
)

pkg_tar(
    name = "twister2-client-lib-api",
    srcs = twister2_client_lib_api_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-lib-task",
    srcs = twister2_client_lib_task_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-lib-data",
    srcs = twister2_client_lib_data_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-lib-data-lmdb",
    srcs = twister2_client_lib_data_lmdb_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-lib-resource-scheduler",
    srcs = twister2_client_lib_resource_scheduler_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-lib-task-scheduler",
    srcs = twister2_client_lib_task_scheduler_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-lib-third_party",
    srcs = twister2_client_lib_third_party_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-communication",
    srcs = twister2_client_lib_communication_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-lib-common",
    srcs = twister2_client_lib_common_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-lib-connector",
    srcs = twister2_client_lib_connector_files(),
    package_dir = "lib",
)

pkg_tar(
    name = "twister2-client-examples",
    srcs = twister2_client_example_files(),
    package_dir = "examples",
)

pkg_tar(
    name = "twister2-master-lib",
    srcs = twister2_master_lib_files(),
    package_dir = "lib",
)
