"""Macros to simplify generating maven files.
"""

#load("@google_bazel_common//tools/maven:pom_file.bzl", default_pom_file = "pom_file")
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

POM_VERSION = "${project.version}"

load("@bazel_tools//tools/build_defs/repo:java.bzl", "java_import_external")

_MAVEN_MIRRORS = [
    "http://bazel-mirror.storage.googleapis.com/repo1.maven.org/maven2/",
    "http://repo1.maven.org/maven2/",
    "http://maven.ibiblio.org/maven2/",
]

def maven_import(artifact, sha256, licenses, **kwargs):
    parts = artifact.split(":")
    group_id = parts[0]
    artifact_id = parts[1]
    version = parts[2]
    name = ("%s_%s" % (group_id, artifact_id)).replace(".", "_").replace("-", "_")
    url_suffix = "{0}/{1}/{2}/{1}-{2}.jar".format(group_id.replace(".", "/"), artifact_id, version)

    java_import_external(
        name = name,
        jar_urls = [base + url_suffix for base in _MAVEN_MIRRORS],
        jar_sha256 = sha256,
        licenses = licenses,
        tags = ["maven_coordinates=" + artifact],
        **kwargs
    )
