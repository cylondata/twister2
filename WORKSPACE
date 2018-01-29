maven_server(
  name = "default",
  url = "http://central.maven.org/maven2/",
)

maven_jar(
  name = "org_apache_commons_commons_compress",
  artifact = "org.apache.commons:commons-compress:1.15",
)

maven_jar(
  name = "com_google_protobuf_protobuf_java",
  artifact = "com.google.protobuf:protobuf-java:3.4.0",
)

maven_jar(
  name = "org_yaml_snakeyaml",
  artifact = "org.yaml:snakeyaml:1.15",
)

maven_jar(
  name = "ompi_ompijavabinding",
  artifact = "ompi:ompijavabinding:3.0.0"
)

maven_jar(
  name = "commons_lang_commons_lang",
  artifact = "org.apache.commons:commons-lang3:3.6",
)

maven_jar(
  name = "commons_cli_commons_cli",
  artifact = "commons-cli:commons-cli:1.3.1",
)

maven_jar(
  name = "com_google_guava_guava",
  artifact = "com.google.guava:guava:18.0",
)

maven_jar(
  name = "com_puppycrawl_tools_checkstyle",
  artifact = "com.puppycrawl.tools:checkstyle:6.17",
)

maven_jar(
  name = "commons_collections_commons_collections",
  artifact = "commons-collections:commons-collections:3.2.1",
)

maven_jar(
  name = "antlr_antlr",
  artifact = "antlr:antlr:2.7.7",
)

maven_jar(
  name = "commons_beanutils_commons_beanutils",
  artifact = "commons-beanutils:commons-beanutils:1.9.2",
)

maven_jar(
  name = "commons_logging_commons_logging",
  artifact = "commons-logging:commons-logging:1.1.1",
)

maven_jar(
  name = "commons_io_commons_io",
  artifact = "commons-io:commons-io:2.5",
)

maven_jar(
  name = "org_slf4j_slf4j_api",
  artifact = "org.slf4j:slf4j-api:1.7.7"
)

maven_jar(
  name = "org_slf4j_slf4j_jdk14",
  artifact = "org.slf4j:slf4j-jdk14:1.7.7"
)

maven_jar(
  name = "com_esotericsoftware_kryo",
  artifact = "com.esotericsoftware:kryo:3.0.3",
)

maven_jar(
  name = "org_objenesis_objenesis",
  artifact = "org.objenesis:objenesis:2.1",
)

maven_jar(
  name = "com_esotericsoftware_minlog",
  artifact = "com.esotericsoftware:minlog:1.3.0",
)

maven_jar(
  name = "com_esotericsoftware_reflectasm",
  artifact = "com.esotericsoftware:reflectasm:1.10.0",
)

maven_jar(
  name = "org_ow2_asm_asm",
  artifact = "org.ow2.asm:asm:4.2",
)

maven_jar(
  name = "akka_actor",
  artifact = "com.typesafe.akka:akka-actor:2.5.6",
)

maven_jar(
  name = "akka_remote",
  artifact = "com.typesafe.akka:akka-remote:2.5.6",
)

maven_jar(
  name ="net_openhft_chronicle_queue",
  artifact = "net.openhft:chronicle-queue:4.6.55",
)

maven_jar(
  name ="lmdb_java",
  artifact = "org.lmdbjava:lmdbjava:0.6.0",
)


maven_jar(
  name ="lmdbjava_native_linux",
  artifact = "org.lmdbjava:lmdbjava-native-linux-x86_64:0.9.21-1",
)

maven_jar(
  name ="lmdbjava_native_windows",
  artifact = "org.lmdbjava:lmdbjava-native-windows-x86_64:0.9.21-1",
)

maven_jar(
  name ="lmdbjava_native_osx",
  artifact = "org.lmdbjava:lmdbjava-native-osx-x86_64:0.9.21-1",
)

maven_jar(
  name ="com_github_jnr_ffi",
  artifact = "com.github.jnr:jnr-ffi:2.1.7",
)

maven_jar(
  name ="com_github_jnr_constants",
  artifact = "com.github.jnr:jnr-constants:0.9.9",
)

maven_jar(
  name ="com_github_jnr_jffi",
  artifact = "com.github.jnr:jffi:1.2.16",
)

http_archive(
    name = "com_google_protobuf",
    urls = ["https://github.com/google/protobuf/archive/v3.4.1.tar.gz"],
    strip_prefix = "protobuf-3.4.1",
)

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