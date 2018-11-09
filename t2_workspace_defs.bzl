#reference : https://github.com/google/bazel-common/blob/master/workspace_defs.bzl

load("@bazel_tools//tools/build_defs/repo:java.bzl", "java_import_external")

_MAVEN_MIRRORS = [
    "http://bazel-mirror.storage.googleapis.com/repo1.maven.org/maven2/",
    "http://repo1.maven.org/maven2/",
    "http://maven.ibiblio.org/maven2/",
]

def _maven_import(artifact, sha256, licenses, **kwargs):
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

def load_modules():
    #Protocol Buffers
    _maven_import(
        artifact = "com.google.protobuf:protobuf-java:3.5.0",
        licenses = ["notice"],
        sha256 = "49a3c7b3781d4b7b2d15063e125824260c9b46bdb62494b63b367b661fdb2b26",
    )

    native.http_archive(
        name = "com_google_protobuf",
        sha256 = "cef7f1b5a7c5fba672bec2a319246e8feba471f04dcebfe362d55930ee7c1c30",
        strip_prefix = "protobuf-3.5.0",
        urls = ["https://github.com/google/protobuf/archive/v3.5.0.zip"],
    )

    #Guava
    _maven_import(
        artifact = "com.google.guava:guava:25.0-jre",
        licenses = ["notice"],
        sha256 = "3fd4341776428c7e0e5c18a7c10de129475b69ab9d30aeafbb5c277bb6074fa9",
    )

    #Skylib
    skylib_version = "9430df29e4c648b95bf39a57e4336b44a0a0582a"

    native.http_archive(
        name = "bazel_skylib",
        strip_prefix = "bazel-skylib-{}".format(skylib_version),
        urls = ["https://github.com/bazelbuild/bazel-skylib/archive/{}.zip".format(skylib_version)],
    )
