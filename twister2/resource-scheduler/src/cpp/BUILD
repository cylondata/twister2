package(default_visibility = ["//visibility:public"])

cc_library(
    name = "standalone_cxx",
    srcs = [
        "standalone/mpi_worker.cpp",
        "core/twister2_submitter.cpp",

        "core/twister2_submitter.h",
        "standalone/mpi_worker.h",
    ],
    copts = [
        "-Itwister2",
        "-Itwister2/resource-scheduler/src/cpp",
        "-I$(GENDIR)/twister2",
        "-I$(GENDIR)/twister2/resource-scheduler/src/cpp",
        "-I$(GENDIR)/twister2/api/src/cpp",
        "-Itwister2/api/src/cpp",
    ],
    deps = [
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
        "@com_github_google_glog//:glog",
        "//twister2/api/src/cpp:api_cxx"
    ],
    linkopts = ["dl"],
    linkstatic = 1,
)

cc_binary(
    name = "standalone_worker",
    srcs = [
        "standalone/mpi_worker.cpp",
        "core/twister2_submitter.cpp",

        "core/twister2_submitter.h",
        "standalone/mpi_worker.h",
    ],
    copts = [
        "-Itwister2",
        "-Itwister2/resource-scheduler/src/cpp",
        "-I$(GENDIR)/twister2",
        "-I$(GENDIR)/twister2/resource-scheduler/src/cpp",
        "-I$(GENDIR)/twister2/api/src/cpp",
        "-Itwister2/api/src/cpp",
    ],
    deps = [
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
        "@com_github_google_glog//:glog",
        "//twister2/api/src/cpp:api_cxx",
    ],
    linkstatic = 1,
)