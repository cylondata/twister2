package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")

genrule(
    name = "web-app",
    srcs = [],
    outs = ["build"],
    cmd = "./$(location build.sh)",
    tools = [
        "build.sh",
        ":node_modules",
    ],
)

filegroup(
    name = "node_modules",
    srcs = glob(
        [
            "node_modules/**/*",
            "node_modules/**/*",
            "src/*",
        ],
        exclude = ["node_modules/**/test/**"],
    ),
)

nodejs_binary(
    name = "parcel",
    entry_point = "react-scripts/scripts/build.js",
    node_modules = ":node_modules",
    # data =  glob(["src/**", "public/**"]) + ["package.json", ".babelrc"],
    templated_args = ["--node_options=--preserve-symlinks"],
)

#filegroup(
#    name = "node_modules",
#    srcs = glob(
#        include = ["node_modules/**/*"],
#        exclude = [
#            # Files under test & docs may contain file names that
#            # are not legal Bazel labels (e.g.,
#            # node_modules/ecstatic/test/public/中文/檔案.html)
#            "node_modules/test/**",
#            "node_modules/docs/**",
#            # Files with spaces are not allowed in Bazel runfiles
#            # See https://github.com/bazelbuild/bazel/issues/4327
#            "node_modules/**/* */**",
#            "node_modules/**/* *",
#        ],
#    ),
#)
