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

def t2_java_lib(name, srcs = [], deps = [], artifact_name = "", generate_pom = True):
    native.java_library(
        name = name,
        srcs = srcs,
        deps = deps,
        tags = mvn_tag("edu.iu.dsc.tws", name, T2_VERSION),
    )

    if (generate_pom):
        pom_file(
            name = "pom",
            artifact_id = name,
            artifact_name = artifact_name,
            targets = [":%s" % name],
        )
