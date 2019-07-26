jackson_version = "2.8.8"

powermock_version = "1.6.2"

PEX_SRC = "https://pypi.python.org/packages/3a/1d/cd41cd3765b78a4353bbf27d18b099f7afbcd13e7f2dc9520f304ec8981c/pex-1.2.15.tar.gz"

REQUESTS_SRC = "https://pypi.python.org/packages/d9/03/155b3e67fe35fe5b6f4227a8d9e96a14fda828b18199800d161bcefc1359/requests-2.12.3.tar.gz"

SETUPTOOLS_SRC = "https://pypi.python.org/packages/68/13/1bfbfbd86560e61fa9803d241084fff41a775bf56ee8b3ad72fc9e550dad/setuptools-31.0.0.tar.gz"

VIRTUALENV_SRC = "https://pypi.python.org/packages/d4/0c/9840c08189e030873387a73b90ada981885010dd9aea134d6de30cd24cb8/virtualenv-15.1.0.tar.gz"

VIRTUALENV_PREFIX = "virtualenv-15.1.0"

WHEEL_SRC = "https://pypi.python.org/packages/c9/1d/bd19e691fd4cfe908c76c429fe6e4436c9e83583c4414b54f6c85471954a/wheel-0.29.0.tar.gz"

PYTEST_WHEEL = "https://pypi.python.org/packages/fd/3e/d326a05d083481746a769fc051ae8d25f574ef140ad4fe7f809a2b63c0f0/pytest-3.1.3-py2.py3-none-any.whl"

PY_WHEEL = "https://pypi.python.org/packages/53/67/9620edf7803ab867b175e4fd23c7b8bd8eba11cb761514dcd2e726ef07da/py-1.4.34-py2.py3-none-any.whl"



load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

http_file(
    name = "pytest_whl",
    downloaded_file_path = "pytest-3.1.3-py2.py3-none-any.whl",
    urls = [PYTEST_WHEEL],
)

http_file(
    name = "py_whl",
    downloaded_file_path = "py-1.4.34-py2.py3-none-any.whl",
    urls = [PY_WHEEL],
)

http_file(
    name = "wheel_src",
    downloaded_file_path = "wheel-0.29.0.tar.gz",
    urls = [WHEEL_SRC],
)

http_file(
    name = "pex_src",
    downloaded_file_path = "pex-1.2.15.tar.gz",
    urls = [PEX_SRC],
)

http_file(
    name = "requests_src",
    downloaded_file_path = "requests-2.12.3.tar.gz",
    urls = [REQUESTS_SRC],
)

http_file(
    name = "setuptools_src",
    downloaded_file_path = "setuptools-31.0.0.tar.gz",
    urls = [SETUPTOOLS_SRC],
)

http_archive(
    name = "virtualenv",
    urls = [VIRTUALENV_SRC],
    strip_prefix = VIRTUALENV_PREFIX,
    build_file_content = "\n".join([
        "py_binary(",
        "    name = 'virtualenv',",
        "    srcs = ['virtualenv.py'],",
        "    data = glob(['**/*']),",
        "    visibility = ['//visibility:public'],",
        ")",
    ]),
    sha256 = "02f8102c2436bb03b3ee6dede1919d1dac8a427541652e5ec95171ec8adbc93a",
)


# for nomad repo
http_archive(
    name = "nomad_mac",
    build_file = "@//:third_party/nomad/nomad.BUILD",
    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_darwin_amd64.zip"],
)

http_archive(
    name = "nomad_linux",
    build_file = "@//:third_party/nomad/nomad.BUILD",
    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_linux_amd64.zip"],
)

#http_archive(
#    name = "nomad_mac",
#    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_darwin_amd64.zip"],
#    build_file = "@//:third_party/nomad/nomad.BUILD",
#    sha256 = "53452f5bb27131f1fe5a5f9178324511bcbc54e4fef5bec4e25b049ac38e0632",
#)

#http_archive(
#    name = "nomad_linux",
#    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_linux_amd64.zip"],
#    build_file = "@//:third_party/nomad/nomad.BUILD",
#    sha256 = "b3b78dccbdbd54ddc7a5ffdad29bce2d745cac93ea9e45f94e078f57b756f511",
#)

http_archive(
    name = "ompi3",
    build_file = "@//:third_party/ompi3/ompi.BUILD",
    strip_prefix = "openmpi-3.1.2",
    urls = ["https://github.com/DSC-SPIDAL/twister2-thridparty-bin/raw/master/mpi/openmpi-3.1.2.tar.gz"],
)

http_archive(
    name = "ompi3darwin",
    build_file = "@//:third_party/ompi3darwin/ompi.darwin.BUILD",
    strip_prefix = "openmpi-3.1.2",
    urls = ["https://github.com/DSC-SPIDAL/twister2-thridparty-bin/raw/master/mpi/openmpi-3.1.2.tar.gz"],
)

load("//:t2_workspace_defs.bzl", "load_modules")

load_modules()

git_repository(
    name = "build_bazel_rules_nodejs",
    remote = "https://github.com/bazelbuild/rules_nodejs.git",
    tag = "0.16.4",  # check for the latest tag when you install
)

load("@build_bazel_rules_nodejs//:package.bzl", "rules_nodejs_dependencies")

rules_nodejs_dependencies()

load("@build_bazel_rules_nodejs//:defs.bzl", "node_repositories")

node_repositories(package_json = ["//dashboard/client:package.json"])

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_install")

npm_install(
    name = "npm",
    package_json = "//dashboard/client:package.json",
    package_lock_json = "//dashboard/client:package-lock.json",
)

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")
