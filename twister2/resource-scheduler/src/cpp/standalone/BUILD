package(default_visibility = ["//visibility:public"])

cc_library(
    name = "standalone_cxx",
    srcs = [
        "mpi_worker.cpp",

        "mpi_worker.h",
    ],
    copts = [
        "-Itwister2",
        "-Itwister2/resource-scheduler/src/cpp",
        "-I$(GENDIR)/twister2",
        "-I$(GENDIR)/twister2/resource-scheduler/src/cpp",
    ],
    deps = [
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
        "@com_github_google_glog//:glog",
    ],
    linkstatic = 1,
)