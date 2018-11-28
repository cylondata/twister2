package(default_visibility = ["//visibility:public"])

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_deb", "pkg_tar")
load("//tools:rules/twister2_client.bzl", "twister2_client_bin_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_conf_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_slurm_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_standalone_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_aurora_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_kubernetes_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_mesos_files")
load("//tools:rules/twister2_client.bzl", "twister2_client_nomad_files")
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
load("//tools:rules/twister2_core.bzl", "twister2_harp_integration_files")
load("//tools:rules/twister2_master.bzl", "twister2_master_lib_files")
load("//tools:rules/twister2_master.bzl", "twister2_master_jersey_files")
load(":pack_functions.bzl", "pack_tw2")
load("//:t2_meta.bzl", "T2_VERSION")

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
        ":twister2-%s" % T2_VERSION,
        ":twister2-core-%s" % T2_VERSION,
    ],
)

filegroup(
    name = "tarpkgs_no_mpi",
    srcs = [
        ":twister2-no-mpi-%s" % T2_VERSION,
        ":twister2-core-no-mpi-%s" % T2_VERSION,
    ],
)

######################################################
# twister2 core packaging
######################################################

pack_tw2(
    name = "twister2-core-no-mpi",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-core",
    version = T2_VERSION,
    deps = [
        ":twister2-core-lib-api",
        ":twister2-core-lib-common",
        ":twister2-core-lib-communication",
        ":twister2-core-lib-connector",
        ":twister2-core-lib-data",
        ":twister2-core-lib-executor",
        ":twister2-core-lib-lmdb",
        ":twister2-core-lib-resource-scheduler",
        ":twister2-core-lib-task",
        ":twister2-core-lib-task-scheduler",
        ":twister2-core-lib-third_party",
        ":twister2-master-lib",
        ":twister2-master-jersey",
    ],
)

pack_tw2(
    name = "twister2-core",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-core",
    version = T2_VERSION,
    deps = [
        ":twister2-core-lib-api",
        ":twister2-core-lib-common",
        ":twister2-core-lib-communication",
        ":twister2-core-lib-connector",
        ":twister2-core-lib-data",
        ":twister2-core-lib-executor",
        ":twister2-core-lib-lmdb",
        ":twister2-core-lib-resource-scheduler",
        ":twister2-core-lib-task",
        ":twister2-core-lib-task-scheduler",
        ":twister2-core-lib-third_party",
        ":twister2-harp-libs",
        ":twister2-master-lib",
        ":twister2-master-jersey",
        ":twister2-ompi-bin",
        ":twister2-ompi-etc",
        ":twister2-ompi-include",
        ":twister2-ompi-lib",
    ],
)

pack_tw2(
    name = "twister2-ompi-lib",
    srcs = ["@ompi3//:ompi-lib-files"],
    package_dir = "ompi/lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-ompi-bin",
    srcs = ["@ompi3//:ompi-bin-files"],
    package_dir = "ompi/bin",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-ompi-include",
    srcs = ["@ompi3//:ompi-include-files"],
    package_dir = "ompi/include",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-ompi-etc",
    srcs = ["@ompi3//:ompi-etc-files"],
    package_dir = "ompi/etc",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-api",
    srcs = twister2_client_lib_api_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-task",
    srcs = twister2_client_lib_task_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-resource-scheduler",
    srcs = twister2_client_lib_resource_scheduler_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-task-scheduler",
    srcs = twister2_client_lib_task_scheduler_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-third_party",
    srcs = twister2_client_lib_third_party_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-communication",
    srcs = twister2_client_lib_communication_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-common",
    srcs = twister2_client_lib_common_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-executor",
    srcs = twister2_client_lib_executor_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-connector",
    srcs = twister2_client_lib_connector_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-data",
    srcs = twister2_client_lib_data_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-core-lib-lmdb",
    srcs = twister2_core_lib_data_lmdb_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

######################################################
# twister2 client packaging
######################################################

pack_tw2(
    name = "twister2-no-mpi",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-%s" % T2_VERSION,
    version = T2_VERSION,
    deps = [
        ":twister2-bin",
        ":twister2-communication",
        ":twister2-conf-aurora",
        ":twister2-conf-kubernetes",
        ":twister2-conf-mesos",
        ":twister2-conf-nomad",
        ":twister2-conf-slurm",
        ":twister2-conf-standalone",
        ":twister2-core-lib-executor",
        ":twister2-dist",
        ":twister2-examples",
        ":twister2-harp-libs",
        ":twister2-lib-api",
        ":twister2-lib-common",
        ":twister2-lib-connector",
        ":twister2-lib-data",
        ":twister2-lib-data-lmdb",
        ":twister2-lib-resource-scheduler",
        ":twister2-lib-task",
        ":twister2-lib-task-scheduler",
        ":twister2-lib-third_party",
        ":twister2-master-lib",
        ":twister2-master-jersey",
    ],
)

pack_tw2(
    name = "twister2",
    extension = "tar.gz",
    # srcs = generated_release_files,
    package_dir = "twister2-%s" % T2_VERSION,
    version = T2_VERSION,
    deps = [
        ":twister2-bin",
        ":twister2-communication",
        ":twister2-conf-aurora",
        ":twister2-conf-kubernetes",
        ":twister2-conf-mesos",
        ":twister2-conf-nomad",
        ":twister2-conf-slurm",
        ":twister2-conf-standalone",
        ":twister2-core-lib-executor",
        ":twister2-dist",
        ":twister2-examples",
        ":twister2-harp-libs",
        ":twister2-lib-api",
        ":twister2-lib-common",
        ":twister2-lib-connector",
        ":twister2-lib-data",
        ":twister2-lib-data-lmdb",
        ":twister2-lib-resource-scheduler",
        ":twister2-lib-task",
        ":twister2-lib-task-scheduler",
        ":twister2-lib-third_party",
        ":twister2-master-lib",
        ":twister2-master-jersey",
        ":twister2-ompi-bin",
        ":twister2-ompi-etc",
        ":twister2-ompi-include",
        ":twister2-ompi-lib",
    ],
)

pack_tw2(
    name = "twister2-conf-slurm",
    srcs = twister2_client_slurm_files(),
    package_dir = "conf/slurm",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-conf-standalone",
    srcs = twister2_client_standalone_files(),
    package_dir = "conf/standalone",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-conf-aurora",
    srcs = twister2_client_aurora_files(),
    package_dir = "conf/aurora",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-conf-kubernetes",
    srcs = twister2_client_kubernetes_files(),
    package_dir = "conf/kubernetes",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-conf-mesos",
    srcs = twister2_client_mesos_files(),
    package_dir = "conf/mesos",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-conf-nomad",
    srcs = twister2_client_nomad_files(),
    package_dir = "conf/nomad",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-dist",
    srcs = [":twister2-core-%s" % T2_VERSION],
    package_dir = "dist",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-bin",
    srcs = twister2_client_bin_files(),
    package_dir = "bin",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-api",
    srcs = twister2_client_lib_api_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-task",
    srcs = twister2_client_lib_task_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-data",
    srcs = twister2_client_lib_data_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-data-lmdb",
    srcs = twister2_client_lib_data_lmdb_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-resource-scheduler",
    srcs = twister2_client_lib_resource_scheduler_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-task-scheduler",
    srcs = twister2_client_lib_task_scheduler_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-third_party",
    srcs = twister2_client_lib_third_party_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-communication",
    srcs = twister2_client_lib_communication_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-common",
    srcs = twister2_client_lib_common_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-lib-connector",
    srcs = twister2_client_lib_connector_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-examples",
    srcs = twister2_client_example_files(),
    package_dir = "examples",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-master-lib",
    srcs = twister2_master_lib_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-master-jersey",
    srcs = twister2_master_jersey_files(),
    package_dir = "lib",
    version = T2_VERSION,
)

pack_tw2(
    name = "twister2-harp-libs",
    srcs = twister2_harp_integration_files(),
    package_dir = "lib",
    version = T2_VERSION,
)
