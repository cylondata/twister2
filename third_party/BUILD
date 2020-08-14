package(default_visibility = ["//visibility:public"])

java_import(
    name = "ompi_javabinding_java",
    jars = select({
        "//tools/platform:darwin": ["@ompi3darwin//:ompi-jar-files"],
        "//conditions:default": ["@ompi3//:ompi-jar-files"],
    }),
    visibility = ["//visibility:public"],
)

java_import(
    name = "ucx_javabinding_java",
    jars = ["@ucx//:ucx-jar-file"],
    visibility = ["//visibility:public"],
)

java_import(
    name = "cylon_javabinding_java",
    jars = ["@cylon//:cylon-jar-file"],
    visibility = ["//visibility:public"],
)

java_import(
    name = "harp_collective",
    jars = ["harp/harp-collective-0.1.0.jar"],
    visibility = ["//visibility:public"],
)

java_import(
    name = "com_github_jnr_jffi_native",
    jars = ["jnr/jffi-native-1.2.16.jar"],
    visibility = ["//visibility:public"],
)

java_import(
    name = "vendored_guava_26_0_jre",
    jars = ["beamguava26/beam-vendor-guava-26_0-jre-0.1.jar"],
    visibility = ["//visibility:public"],
)

java_import(
    name = "vendored_grpc_1_21_0",
    jars = ["beamgrpc12/beam-vendor-grpc-1_21_0-0.1.jar"],
    visibility = ["//visibility:public"],
)
