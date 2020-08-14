licenses(["notice"])

package(default_visibility = ["//visibility:public"])

out_files = ["cylon-0.1.0-jar-with-dependencies.jar",]

genrule(
    name = "cylon-srcs",
    outs = out_files,
    local = 1,
    cmd = "\n".join([
        'export INSTALL_DIR=$$(pwd)/$(@D)',
        'export TMP_DIR=$$(mktemp -d -t cylon.XXXXXX)',
        'echo $$TMP_DIR',
        'echo $$INSTALL_DIR',
        'mkdir -p $$TMP_DIR',
        'cp -pLR $$(pwd)/external/cylon/* $$TMP_DIR',
        'cd $$TMP_DIR',
        './build.sh -pyenv ENV/ -bpath $$TMP_DIR/build --java',
        'cp $$TMP_DIR/java/target/cylon-0.1.0-jar-with-dependencies.jar $$INSTALL_DIR',
        'rm -rf $$TMP_DIR',
    ]),
)


filegroup(
    name = "cylon-jar-file",
    srcs = out_files,
)

