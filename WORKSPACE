jackson_version = "2.8.8"

powermock_version = "1.6.2"

PEX_SRC = "https://pypi.python.org/packages/3a/1d/cd41cd3765b78a4353bbf27d18b099f7afbcd13e7f2dc9520f304ec8981c/pex-1.2.15.tar.gz"

REQUESTS_SRC = "https://pypi.python.org/packages/d9/03/155b3e67fe35fe5b6f4227a8d9e96a14fda828b18199800d161bcefc1359/requests-2.12.3.tar.gz"

SETUPTOOLS_SRC = "https://pypi.python.org/packages/68/13/1bfbfbd86560e61fa9803d241084fff41a775bf56ee8b3ad72fc9e550dad/setuptools-31.0.0.tar.gz"

VIRTUALENV_SRC = "https://files.pythonhosted.org/packages/aa/3b/213c384c65e17995cccd0f2bb993b7b82c41f62e74c2f8f39c8e60549d86/virtualenv-16.7.9.tar.gz"

VIRTUALENV_PREFIX = "virtualenv-16.7.9"

WHEEL_SRC = "https://pypi.python.org/packages/c9/1d/bd19e691fd4cfe908c76c429fe6e4436c9e83583c4414b54f6c85471954a/wheel-0.29.0.tar.gz"

PYTEST_WHEEL = "https://pypi.python.org/packages/fd/3e/d326a05d083481746a769fc051ae8d25f574ef140ad4fe7f809a2b63c0f0/pytest-3.1.3-py2.py3-none-any.whl"

PY_WHEEL = "https://pypi.python.org/packages/53/67/9620edf7803ab867b175e4fd23c7b8bd8eba11cb761514dcd2e726ef07da/py-1.4.34-py2.py3-none-any.whl"

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

RULES_JVM_EXTERNAL_TAG = "2.2"

RULES_JVM_EXTERNAL_SHA = "f1203ce04e232ab6fdd81897cf0ff76f2c04c0741424d192f28e65ae752ce2d6"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

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
    sha256 = "1ebb8ad7e26b448e9caa4773d2357849bf80ff9e313964bcaf79cbf0201a1648",
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
    build_file_content = "\n".join([
        "py_binary(",
        "    name = 'virtualenv',",
        "    srcs = ['virtualenv.py'],",
        "    data = glob(['**/*']),",
        "    visibility = ['//visibility:public'],",
        ")",
    ]),
    sha256 = "0d62c70883c0342d59c11d0ddac0d954d0431321a41ab20851facf2b222598f3",
    strip_prefix = VIRTUALENV_PREFIX,
    urls = [VIRTUALENV_SRC],
)

# for nomad repo
#http_archive(
#    name = "nomad_mac",
#    build_file = "@//:third_party/nomad/nomad.BUILD",
#    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_darwin_amd64.zip"],
#)

#http_archive(
#    name = "nomad_linux",
#    build_file = "@//:third_party/nomad/nomad.BUILD",
#    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_linux_amd64.zip"],
#)

http_archive(
    name = "nomad_mac",
    build_file = "@//:third_party/nomad/nomad.BUILD",
    sha256 = "53452f5bb27131f1fe5a5f9178324511bcbc54e4fef5bec4e25b049ac38e0632",
    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_darwin_amd64.zip"],
)

http_archive(
    name = "nomad_linux",
    build_file = "@//:third_party/nomad/nomad.BUILD",
    sha256 = "b3b78dccbdbd54ddc7a5ffdad29bce2d745cac93ea9e45f94e078f57b756f511",
    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_linux_amd64.zip"],
)

http_archive(
    name = "ompi3",
    build_file = "@//:third_party/ompi3/ompi.BUILD",
    strip_prefix = "openmpi-4.0.1",
    urls = ["https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.1.tar.gz"],
)

http_archive(
    name = "ompi3darwin",
    build_file = "@//:third_party/ompi3darwin/ompi.darwin.BUILD",
    strip_prefix = "openmpi-4.0.1",
    urls = ["https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.1.tar.gz"],
)

http_archive(
    name = "ucx",
    build_file = "@//:third_party/ucx/ucx.BUILD",
    sha256 = "6bf7d37d921f79e9b2959810c76bb5891b269b2872ed8328990210b09242abe7",
    strip_prefix = "ucx-b57b32df58b3755d876cd4d2da4b27bc54733ccc",
    urls = ["https://github.com/openucx/ucx/archive/b57b32df58b3755d876cd4d2da4b27bc54733ccc.zip"],
)

http_archive(
    name = "cylon",
    build_file = "@//:third_party/cylon/cylon.BUILD",
    strip_prefix = "cylon-a560e3850ae345ec5b1201e95106bf437571bf3f",
    urls = ["https://github.com/cylondata/cylon/archive/a560e3850ae345ec5b1201e95106bf437571bf3f.zip"],
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

##################################
#    PROTOCOL BUFFER RULES       #
##################################

http_archive(
    name = "rules_cc",
    sha256 = "35f2fb4ea0b3e61ad64a369de284e4fbbdcdba71836a5555abb5e194cf119509",
    strip_prefix = "rules_cc-624b5d59dfb45672d4239422fa1e3de1822ee110",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
        "https://github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
    ],
)

# rules_java defines rules for generating Java code from Protocol Buffers.
http_archive(
    name = "rules_java",
    sha256 = "ccf00372878d141f7d5568cedc4c42ad4811ba367ea3e26bc7c43445bbc52895",
    strip_prefix = "rules_java-d7bf804c8731edd232cb061cb2a9fe003a85d8ee",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_java/archive/d7bf804c8731edd232cb061cb2a9fe003a85d8ee.tar.gz",
        "https://github.com/bazelbuild/rules_java/archive/d7bf804c8731edd232cb061cb2a9fe003a85d8ee.tar.gz",
    ],
)

# rules_proto defines abstract rules for building Protocol Buffers.
http_archive(
    name = "rules_proto",
    sha256 = "57001a3b33ec690a175cdf0698243431ef27233017b9bed23f96d44b9c98242f",
    strip_prefix = "rules_proto-9cd4f8f1ede19d81c6d48910429fe96776e567b1",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/9cd4f8f1ede19d81c6d48910429fe96776e567b1.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/9cd4f8f1ede19d81c6d48910429fe96776e567b1.tar.gz",
    ],
)

# java_lite_proto_library rules implicitly depend on @com_google_protobuf_javalite//:javalite_toolchain,
# which is the JavaLite proto runtime (base classes and common utilities).
http_archive(
    name = "com_google_protobuf_javalite",
    sha256 = "a8cb9b8db16aff743a4bc8193abec96cf6ac0b0bc027121366b43ae8870f6fd3",
    strip_prefix = "protobuf-fa08222434bc58d743e8c2cc716bc219c3d0f44e",
    urls = [
        "https://github.com/google/protobuf/archive/fa08222434bc58d743e8c2cc716bc219c3d0f44e.zip",
    ],
)

load("@rules_cc//cc:repositories.bzl", "rules_cc_dependencies")

rules_cc_dependencies()

load("@rules_java//java:repositories.bzl", "rules_java_dependencies", "rules_java_toolchains")

rules_java_dependencies()

rules_java_toolchains()

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

##################################
# END OF PROTOCOL BUFFER RULES   #
##################################
