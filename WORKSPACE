jackson_version = "2.8.8"

powermock_version = "1.6.2"

PEX_SRC = "https://pypi.python.org/packages/3a/1d/cd41cd3765b78a4353bbf27d18b099f7afbcd13e7f2dc9520f304ec8981c/pex-1.2.15.tar.gz"

REQUESTS_SRC = "https://pypi.python.org/packages/d9/03/155b3e67fe35fe5b6f4227a8d9e96a14fda828b18199800d161bcefc1359/requests-2.12.3.tar.gz"

SETUPTOOLS_SRC = "https://pypi.python.org/packages/68/13/1bfbfbd86560e61fa9803d241084fff41a775bf56ee8b3ad72fc9e550dad/setuptools-31.0.0.tar.gz"

VIRTUALENV_SRC = "https://pypi.python.org/packages/d4/0c/9840c08189e030873387a73b90ada981885010dd9aea134d6de30cd24cb8/virtualenv-15.1.0.tar.gz"

VIRTUALENV_PREFIX = "virtualenv-15.1.0"

WHEEL_SRC = "https://pypi.python.org/packages/c9/1d/bd19e691fd4cfe908c76c429fe6e4436c9e83583c4414b54f6c85471954a/wheel-0.29.0.tar.gz"

http_file(
    name = "setuptools_src",
    url = SETUPTOOLS_SRC,
)

new_http_archive(
    name = "virtualenv",
    url = VIRTUALENV_SRC,
    strip_prefix = VIRTUALENV_PREFIX,
    build_file_content = "\n".join([
        "py_binary(",
        "    name = 'virtualenv',",
        "    srcs = ['virtualenv.py'],",
        "    data = glob(['**/*']),",
        "    visibility = ['//visibility:public'],",
        ")",
    ])
)

http_file(
    name = "pex_src",
    url = PEX_SRC,
)

http_file(
    name = "requests_src",
    url = REQUESTS_SRC,
)

http_file(
    name = "wheel_src",
    url = WHEEL_SRC,
)

# for nomad repo
new_http_archive(
    name = "nomad_mac",
    build_file = "third_party/nomad/nomad.BUILD",
    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_darwin_amd64.zip"],
)

new_http_archive(
    name = "nomad_linux",
    build_file = "third_party/nomad/nomad.BUILD",
    urls = ["https://releases.hashicorp.com/nomad/0.7.0/nomad_0.7.0_linux_amd64.zip"],
)

new_http_archive(
    name = "ompi3",
    build_file = "third_party/ompi3/ompi.BUILD",
    strip_prefix = "openmpi-3.1.2",
    urls = ["https://download.open-mpi.org/release/open-mpi/v3.1/openmpi-3.1.2.tar.gz"],
)

load("//:t2_workspace_defs.bzl", "load_modules")

load_modules()
