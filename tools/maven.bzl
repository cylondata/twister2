"""Macros to simplify generating maven files.
"""

load("//:t2_meta.bzl", "T2_VERSION")
load("//tools:pom_file.bzl", default_pom_file = "pom_file")

def pom_file(name, targets, artifact_name, artifact_id, packaging = None, **kwargs):
    default_pom_file(
        name = name,
        targets = targets,
        preferred_group_ids = [
            "edu.iu.dsc.tws",
            "edu.iu",
        ],
        template_file = "//tools:pom-template.xml",
        substitutions = {
            "{artifact_name}": artifact_name,
            "{artifact_id}": artifact_id,
            "{packaging}": packaging or "jar",
        },
        **kwargs
    )

def mvn_tag(group_id, artifact_id, version):
    return ["maven_coordinates=" + group_id + ":" + artifact_id + ":" + version]

def t2_java_lib(
        name,
        srcs = [],
        deps = [],
        artifact_name = "",
        generate_pom = True,
        resource_jars = []):
    native.java_library(
        name = name,
        srcs = srcs,
        deps = deps,
        tags = mvn_tag("org.twister2", name, T2_VERSION),
        resource_jars = resource_jars,
    )

    native.genrule(
        name = name + "-javadoc",
        srcs = [":" + name],
        outs = [name + "-javadoc.jar"],
        cmd = "cp $(SRCS) $@",
    )

    if (generate_pom):
        pom_file(
            name = "pom",
            artifact_id = name,
            artifact_name = artifact_name,
            targets = [":%s" % name],
        )

def t2_proto_java_lib(name, srcs = [], deps = [], artifact_name = "", generate_pom = True):
    # generate java code
    native.java_proto_library(
        name = name,
        deps = deps,
        tags = mvn_tag("org.twister2", name, T2_VERSION),
    )

    native.java_library(
        name = "j%s" % name,
        srcs = srcs,
        deps = [":%s" % name],
        tags = mvn_tag("org.twister2", name, T2_VERSION),
        resource_jars = [":%s" % name],
    )

    native.genrule(
        name = "j%s" % name + "-javadoc",
        srcs = [":" + ("j%s" % name)],
        outs = [("j%s" % name) + "-javadoc.jar"],
        cmd = "cp $(SRCS) $@",
    )

    if (generate_pom):
        pom_file(
            name = "pom",
            artifact_id = name,
            artifact_name = artifact_name,
            targets = [":j%s" % name],
        )
