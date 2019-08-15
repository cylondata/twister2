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
    name = "harp_collective",
    jars = ["harp/harp-collective-0.1.0.jar"],
    visibility = ["//visibility:public"],
)

java_import(
    name = "com_github_jnr_jffi_native",
    jars = ["jnr/jffi-native-1.2.16.jar"],
    visibility = ["//visibility:public"],
)
