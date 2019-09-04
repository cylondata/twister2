package(default_visibility = ["//visibility:public"])

cc_library(
    name = "api_cxx",
    srcs = [
        "config/config.cpp",
        "twister2_job.cpp",

        "resource/worker.h",
        "config/config.h",
        "twister2_job.h",
    ],
    hdrs = [
        "resource/worker.h",
        "config/config.h",
        "twister2_job.h",
    ],
    copts = [
        "-Itwister2",
        "-Itwister2/api/src/cpp",
        "-I$(GENDIR)/twister2",
        "-I$(GENDIR)/twister2/api/src/cpp",
    ],
    deps = [
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
        "@com_github_google_glog//:glog",
    ],
    linkstatic = 1,
)