load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_deb", "pkg_tar")

def pack_tw2(name, package_dir, version, srcs = [], extension = "tar", deps = []):
    versioned_deps = ["%s-%s" % (dep, version) for dep in deps]
    pkg_tar(name = "%s-%s" % (name, version), srcs = srcs, extension = extension, package_dir = package_dir, deps = versioned_deps)
