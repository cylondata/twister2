package(default_visibility = ["//visibility:public"])

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar", "pkg_deb")

load("/tools/rules/twister2_client", "twister2_client_bin_files")
load("/tools/rules/twister2_client", "twister2_client_conf_files")
load("/tools/rules/twister2_client", "twister2_client_lib_third_party_files")
load("/tools/rules/twister2_client", "twister2_client_lib_task_scheduler_files")
load("/tools/rules/twister2_client", "twister2_client_lib_resource_scheduler_files")
load("/tools/rules/twister2_client", "twister2_client_lib_api_files")

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
    # files = generated_release_files,
    deps = [
        ":twister2-core-lib-resource-scheduler",
        ":twister2-core-lib-task-scheduler",
        ":twister2-core-lib-communication",
    ],
)

pkg_tar(
    name = "twister2-core-lib-resource-scheduler",
    package_dir = "twister2-core/lib/resource-scheduler",
    files = twister2_core_lib_resource_scheduler_files(),
)

pkg_tar(
    name = "twister2-core-lib-task-scheduler",
    package_dir = "twister2-core/lib/task-scheduler",
    files = twister2_core_lib_task_scheduler_files(),
)

pkg_tar(
    name = "twister2-core-lib-communication",
    package_dir = "twister2-core/lib/communication",
    files = twister2_core_lib_communication_files(),
)

# twister2 client packaging

pkg_tar(
    name = "twister2-client",
    extension = "tar.gz",
    # files = generated_release_files,
    deps = [
        ":twister2-client-bin",
        ":twister2-client-conf",
        ":twister2-client-dist",
        ":twister2-client-lib-third_party",
        ":twister2-client-lib-resource-scheduler",
        ":twister2-client-lib-task-scheduler",
        ":twister2-client-lib-api",
    ],
)

pkg_tar(
    name = "twister2-client-conf",
    package_dir = "conf",
    files = twister2_client_conf_files(),
)

pkg_tar(
    name = "twister2-client-dist",
    package_dir = "dist",
    files = [":twister2-core"],
)

pkg_tar(
    name = "twister2-client-bin",
    package_dir = "twister2-client/bin",
    files = twister2_client_bin_files(),
)

pkg_tar(
    name = "twister2-client-lib-api",
    package_dir = "twister2-client/lib/api",
    files = twister2_client_lib_api_files(),
)

pkg_tar(
    name = "twister2-client-lib-resource-scheduler",
    package_dir = "twister2-client/lib/resource-scheduler",
    files = twister2_client_lib_resource_scheduler_files(),
)

pkg_tar(
    name = "twister2-client-lib-task-scheduler",
    package_dir = "twister2-client/lib/task-scheduler",
    files = twister2_client_lib_task_scheduler_files(),
)

pkg_tar(
    name = "twister2-client-lib-third_party",
    package_dir = "twister2-client/lib/third_party",
    files = twister2_client_lib_third_party_files(),
)
