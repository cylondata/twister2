package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "cli-py",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    deps = [
    ],
    reqs = [
        "pyyaml==3.10",
        "enum34==1.1.6",
        'psutil==4.3.1',
        "colorlog==2.6.1",
    ],
)

pex_binary(
    name = "twister2",
    srcs = [
        "main.py",
    ],
    deps = [":cli-py"],
)