package(default_visibility = ["//visibility:public"])

load("//tools:maven.bzl", "t2_java_lib")

t2_java_lib(
    name = "comms-api-java",
    srcs = glob(["**/*.java"]),
    artifact_name = "Twiter2 Communication API",
    deps = [
        "//third_party:ompi_javabinding_java",  #todo get rid of this
        "//twister2/api/src/java/edu/iu/dsc/tws/api/config:config-api-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/util:api-utils-java",
        "//twister2/api/src/java/edu/iu/dsc/tws/api/faulttolerance:fault-tolerance-api-java",
    ],
)
