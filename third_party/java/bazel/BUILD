licenses(["notice"])

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "extra_actions_proto",
    srcs = ["extra_actions_base.proto"],
    proto_source_root = "third_party/java/bazel",
)


java_proto_library(
    name = "extra_actions_proto_java",
    deps = [":extra_actions_proto"],
)