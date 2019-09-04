package(default_visibility = ["//visibility:public"])

cc_library(
    name = "config_cxx",
    srcs = [
        "config-reader.cpp",
        "core-config-reader.cpp",
        "network-config-reader.cpp",

        "config-reader.h",
        "core-config-reader.h",
        "network-config-reader.h",
    ],
    hdrs = [
        "config-reader.h",
        "core-config-reader.h",
        "network-config-reader.h",
    ],
    copts = [
        "-Itwister2",
        "-Itwister2/common/src/cpp",
        "-I$(GENDIR)/twister2",
        "-I$(GENDIR)/twister2/common/src/cpp",
    ],
    deps = [
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
        "@com_github_google_glog//:glog",
    ],
    linkstatic = 1,
)