licenses(["notice"])  # apache 2.0

package(default_visibility = ["//visibility:public"])

load("//tools/rules/pex:pex_rules.bzl", "pex_binary", "pex_library")

pex_binary(
    name = "pylint",
    srcs = ["main.py"],
    main = "main.py",
    reqs = [
        "pylint==2.3.1",
        "astroid==2.3.1",
        "lazy-object-proxy==1.4.*",
    ],
)
