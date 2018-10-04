package(default_visibility = ["//visibility:public"])

load("//tools/rules/pex:pex_rules.bzl", "pex_binary", "pex_library")

pex_library(
    name = "cli-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    reqs = [
        "pyyaml==3.10",
        "enum34==1.1.6",
        "psutil==4.3.1",
        "colorlog==2.6.1",
    ],
    deps = [
    ],
)

pex_binary(
    name = "twister2",
    srcs = [
        "main.py",
    ],
    deps = [":cli-py"],
)
