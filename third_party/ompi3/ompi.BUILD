licenses(["notice"])

package(default_visibility = ["//visibility:public"])

out_files = ['bin/mpicc'] + glob(['lib/*'])

genrule(
    name = "ompi-srcs",
    outs = out_files,
    local = 1,
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t ompi.XXXXX)',
        'mkdir -p $$TMP_DIR',
        'cp -R $$(pwd)/external/ompi3/* $$TMP_DIR',
        'cd $$TMP_DIR',
        'cp /usr/bin/libtool .',
        './configure --prefix=$$INSTALL_DIR --enable-mpi-java',
        'make;make install',
        'rm -rf $$TMP_DIR',
    ]),
)

filegroup(
    name = "ompi-files",
    srcs = out_files,
)