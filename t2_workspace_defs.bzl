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

    #print(name)
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

    #Generated MVN artifacts
    _maven_import(artifact = "org.powermock:powermock-module-junit4-common:1.6.2", licenses = ["notice"], sha256 = "d3911d010a954ddd912d6d4f5dde5eed0bd6535936654c69a9b63789a0b08723")
    _maven_import(artifact = "com.squareup.okhttp:logging-interceptor:2.7.5", licenses = ["notice"], sha256 = "995576e55173cca3bf4fd472c128d1dca2675d9f56f76320b74c4d6ddbd3d600")
    _maven_import(artifact = "org.powermock:powermock-api-support:1.6.2", licenses = ["notice"], sha256 = "89e32d0c53dac114ea5e6506b140cf441a7964bde7abba6caacaa3cffa09f0ea")
    _maven_import(artifact = "org.slf4j:slf4j-jdk14:1.7.7", licenses = ["notice"], sha256 = "9909915e991269e5f3f4d8eb51fb0a1da7ff8bc6174a4cf25970f373abafcf9b")
    _maven_import(artifact = "com.esotericsoftware:minlog:1.3.0", licenses = ["notice"], sha256 = "f7b399d3a5478a4f3e0d98bd1c9f47766119c66414bc33aa0f6cde0066f24cc2")
    _maven_import(artifact = "io.swagger:swagger-annotations:1.5.12", licenses = ["notice"], sha256 = "3607ffca7ceaca1f5257a454c322237ea4559f1a0c906da2634864b52215d9c0")
    _maven_import(artifact = "com.squareup.okhttp:okhttp-ws:2.7.5", licenses = ["notice"], sha256 = "32e9f9708f08c794fd04aa96e1b9d66b7fd5332899f534e40027696e33788f71")
    _maven_import(artifact = "org.slf4j:slf4j-api:1.7.25", licenses = ["notice"], sha256 = "18c4a0095d5c1da6b817592e767bb23d29dd2f560ad74df75ff3961dbde25b79")
    _maven_import(artifact = "org.powermock:powermock-reflect:1.6.2", licenses = ["notice"], sha256 = "94c0ea545990f1e439de77e4b6dafe32090d6276eb43a99df9e50c6c8845d57d")
    _maven_import(artifact = "org.powermock:powermock-module-junit4:1.6.2", licenses = ["notice"], sha256 = "c0cbdaa81a19b93095909de41afedeb7d499b828984a4511a6f20d937a70a67c")
    _maven_import(artifact = "org.powermock:powermock-api-mockito:1.6.2", licenses = ["notice"], sha256 = "a5e0be1d52982c81b9c0169622a9ef66d9398eaefd858b43029d16b7a773b7df")
    _maven_import(artifact = "commons-logging:commons-logging:1.1.1", licenses = ["notice"], sha256 = "ce6f913cad1f0db3aad70186d65c5bc7ffcc9a99e3fe8e0b137312819f7c362f")
    _maven_import(artifact = "org.lmdbjava:lmdbjava-native-osx-x86_64:0.9.21-1", licenses = ["notice"], sha256 = "5ebe0302edd76cb63b25fdfea86f75af1e917c0c0355e12f939fccb3c83409bc")
    _maven_import(artifact = "org.ow2.asm:asm:4.2", licenses = ["notice"], sha256 = "3c7e45fe303bd02193d951df134255033b9d8147e77508d09703bac245e6cd9b")
    _maven_import(artifact = "io.kubernetes:client-java:3.0.0", licenses = ["notice"], sha256 = "778f965df7c036a7f6b81b0f56fe4569b62a91792f6d1366aa5bc712dbec4357")
    _maven_import(artifact = "com.squareup.okio:okio:1.6.0", licenses = ["notice"], sha256 = "114bdc1f47338a68bcbc95abf2f5cdc72beeec91812f2fcd7b521c1937876266")
    _maven_import(artifact = "junit:junit:4.11", licenses = ["notice"], sha256 = "90a8e1603eeca48e7e879f3afbc9560715322985f39a274f6f6070b43f9d06fe")
    _maven_import(artifact = "com.google.code.findbugs:jsr305:3.0.0", licenses = ["notice"], sha256 = "bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a")
    _maven_import(artifact = "com.esotericsoftware:reflectasm:1.10.0", licenses = ["notice"], sha256 = "0eea1d90c566eba2a536b4e7ede0138d3ccde59a001d83e157cbf5649d540975")
    _maven_import(artifact = "com.fasterxml.jackson.core:jackson-annotations:2.8.8", licenses = ["notice"], sha256 = "1ff7b1c91658506f1050b39d1564eb4d5dc63586dd709bad58428a63775d75a8")
    _maven_import(artifact = "commons-cli:commons-cli:1.3.1", licenses = ["notice"], sha256 = "3a2f057041aa6a8813f5b59b695f726c5e85014a703d208d7e1689098e92d8c0")
    _maven_import(artifact = "org.apache.httpcomponents:httpmime:4.4", licenses = ["notice"], sha256 = "91d9abfebb3a404090106e10bc6bb0fd33072375c9df27f0f91a4c613289bd4b")
    _maven_import(artifact = "com.github.jnr:jffi:1.2.16", licenses = ["notice"], sha256 = "7a616bb7dc6e10531a28a098078f8184df9b008d5231bdc5f1c131839385335f")
    _maven_import(artifact = "org.lz4:lz4-java:1.4", licenses = ["notice"], sha256 = "9ed51eb236340cab58780ed7d20741ff812bcb3875beb974fa7cf9ddea272358")
    _maven_import(artifact = "org.apache.hadoop:hadoop-auth:2.9.0", licenses = ["notice"], sha256 = "ea2ed54bb505418f26bf7e9dd3a421f60dd65fdae5f9cabf73d821e5694f7ced")
    _maven_import(artifact = "org.lmdbjava:lmdbjava-native-linux-x86_64:0.9.21-1", licenses = ["notice"], sha256 = "d0905caca075b5b63e598398514253dabc1ee7604fd5e12050513aeb6435ac0d")
    _maven_import(artifact = "com.google.code.gson:gson:2.6.2", licenses = ["notice"], sha256 = "b8545ba775f641f8bba86027f06307152279fee89a46a4006df1bf2f874d4d9d")
    _maven_import(artifact = "net.openhft:chronicle-queue:4.6.55", licenses = ["notice"], sha256 = "cc37d54a902b2e125389de06d7e273c6ed366e92d520e5782eb109d933070e6c")
    _maven_import(artifact = "org.yaml:snakeyaml:1.15", licenses = ["notice"], sha256 = "79ea8aac6590f49ee8390c2f17ed9343079e85b44158a097b301dfee42af86ec")
    _maven_import(artifact = "commons-io:commons-io:2.5", licenses = ["notice"], sha256 = "a10418348d234968600ccb1d988efcbbd08716e1d96936ccc1880e7d22513474")
    _maven_import(artifact = "org.lmdbjava:lmdbjava-native-windows-x86_64:0.9.21-1", licenses = ["notice"], sha256 = "f1fce7ddc18cbd9d3f5637d04ef2891ecb0717b6ad86b2372988a039e06ed1d6")
    _maven_import(artifact = "org.apache.curator:curator-recipes:4.0.0", licenses = ["notice"], sha256 = "b039635e54483aa7c8660fd089a1d053f8541eac21c0002e4830569dfdfa61e4")
    _maven_import(artifact = "org.apache.curator:curator-framework:4.0.0", licenses = ["notice"], sha256 = "e3d4a2341dce27a651dbd301742b428596fc9fd5ac0091bdce0c919d47aec6cb")
    _maven_import(artifact = "org.apache.httpcomponents:httpcore:4.4.5", licenses = ["notice"], sha256 = "64d5453874cab7e40a7065cb01a9a9ca1053845a9786b478878b679e0580cec3")
    _maven_import(artifact = "org.javassist:javassist:3.18.1-GA", licenses = ["notice"], sha256 = "3fb71231afd098bb0f93f5eb97aa8291c8d0556379125e596f92ec8f944c6162")
    _maven_import(artifact = "commons-io:commons-io:2.6", licenses = ["notice"], sha256 = "f877d304660ac2a142f3865badfc971dec7ed73c747c7f8d5d2f5139ca736513")
    _maven_import(artifact = "com.fasterxml.jackson.core:jackson-core:2.8.8", licenses = ["notice"], sha256 = "d9bde8c72c22202bf17b05c7811db4964ff8e843d97c00a9bfb048c0fe7a726b")
    _maven_import(artifact = "com.esotericsoftware:kryo:3.0.3", licenses = ["notice"], sha256 = "5c295b23480225ff6e7d6770dfa904bedcec8556c27234fea0a271fe13195f69")
    _maven_import(artifact = "com.hashicorp.nomad:nomad-sdk:0.7.0", licenses = ["notice"], sha256 = "d04dda58d0242f87e66b333f5143d0f3aabb7e8ce1653c0ed1293a648cb18541")
    _maven_import(artifact = "commons-codec:commons-codec:1.11", licenses = ["notice"], sha256 = "e599d5318e97aa48f42136a2927e6dfa4e8881dff0e6c8e3109ddbbff51d7b7d")
    _maven_import(artifact = "commons-configuration:commons-configuration:1.6", licenses = ["notice"], sha256 = "46b71b9656154f6a16ea4b1dc84026b52a9305f8eff046a2b4655fa1738e5eee")
    _maven_import(artifact = "com.github.jnr:jnr-constants:0.9.9", licenses = ["notice"], sha256 = "6862e69646fb726684d8610bc5a65740feab5f235d8d1dc7596113bd1ad54181")
    _maven_import(artifact = "commons-beanutils:commons-beanutils:1.9.2", licenses = ["notice"], sha256 = "23729e3a2677ed5fb164ec999ba3fcdde3f8460e5ed086b6a43d8b5d46998d42")
    _maven_import(artifact = "com.squareup.okhttp:okhttp:2.7.5", licenses = ["notice"], sha256 = "88ac9fd1bb51f82bcc664cc1eb9c225c90dc4389d660231b4cc737bebfe7d0aa")
    _maven_import(artifact = "org.lmdbjava:lmdbjava:0.6.0", licenses = ["notice"], sha256 = "1b02194c292c767fe3b2af8af85c43101cb54d45fbe16c148f78cb89abeb11de")
    _maven_import(artifact = "org.powermock:powermock-core:1.6.2", licenses = ["notice"], sha256 = "48cc45502caa34c017911c6f153b0269dfa731ec706fb196072c8b0d938c4433")
    _maven_import(artifact = "com.google.code.findbugs:jsr305:3.0.2", licenses = ["notice"], sha256 = "766ad2a0783f2687962c8ad74ceecc38a28b9f72a2d085ee438b7813e928d0c7")
    _maven_import(artifact = "org.slf4j:slf4j-api:1.7.7", licenses = ["notice"], sha256 = "69980c038ca1b131926561591617d9c25fabfc7b29828af91597ca8570cf35fe")
    _maven_import(artifact = "org.mockito:mockito-all:1.10.19", licenses = ["notice"], sha256 = "d1a7a7ef14b3db5c0fc3e0a63a81b374b510afe85add9f7984b97911f4c70605")
    _maven_import(artifact = "org.apache.hadoop:hadoop-annotations:2.9.0", licenses = ["notice"], sha256 = "58142c0ee39981e7d40972e58038e93917fcf7b2d818a8b1a623b68c18175d0c")
    _maven_import(artifact = "org.objenesis:objenesis:2.1", licenses = ["notice"], sha256 = "c74330cc6b806c804fd37e74487b4fe5d7c2750c5e15fbc6efa13bdee1bdef80")
    _maven_import(artifact = "org.bouncycastle:bcpkix-jdk15on:1.56", licenses = ["notice"], sha256 = "7043dee4e9e7175e93e0b36f45b1ec1ecb893c5f755667e8b916eb8dd201c6ca")
    _maven_import(artifact = "joda-time:joda-time:2.9.3", licenses = ["notice"], sha256 = "a05f5b8b021802a71919b18702aebdf286148188b3ee9d26e6ec40e8d0071487")
    _maven_import(artifact = "org.apache.commons:commons-compress:1.15", licenses = ["notice"], sha256 = "a778bbd659722889245fc52a0ec2873fbbb89ec661bc1ad3dc043c0757c784c4")
    _maven_import(artifact = "antlr:antlr:2.7.7", licenses = ["notice"], sha256 = "88fbda4b912596b9f56e8e12e580cc954bacfb51776ecfddd3e18fc1cf56dc4c")
    _maven_import(artifact = "org.apache.commons:commons-lang3:3.6", licenses = ["notice"], sha256 = "89c27f03fff18d0b06e7afd7ef25e209766df95b6c1269d6c3ebbdea48d5f284")
    _maven_import(artifact = "org.apache.httpcomponents:httpclient:4.5.2", licenses = ["notice"], sha256 = "0dffc621400d6c632f55787d996b8aeca36b30746a716e079a985f24d8074057")
    _maven_import(artifact = "commons-collections:commons-collections:3.2.2", licenses = ["notice"], sha256 = "eeeae917917144a68a741d4c0dff66aa5c5c5fd85593ff217bced3fc8ca783b8")
    _maven_import(artifact = "com.github.jnr:jnr-ffi:2.1.7", licenses = ["notice"], sha256 = "2ed1bedf59935cd3cc0964bac5cd91638b2e966a82041fe0a6c85f52279c9b34")
    _maven_import(artifact = "org.xerial.snappy:snappy-java:1.1.4", licenses = ["notice"], sha256 = "f75ec0fa9c843e236c6e1512c17c095cfffd175f32e21ea0e3eccb540d77f002")
    _maven_import(artifact = "com.fasterxml.woodstox:woodstox-core:5.0.3", licenses = ["notice"], sha256 = "a1c04b64fbfe20ae9f2c60a3bf1633fed6688ae31935b6bd4a457a1bbb2e82d4")
    _maven_import(artifact = "org.bouncycastle:bcpkix-jdk15on:1.56", licenses = ["notice"], sha256 = "7043dee4e9e7175e93e0b36f45b1ec1ecb893c5f755667e8b916eb8dd201c6ca")
    _maven_import(artifact = "org.apache.zookeeper:zookeeper:3.4.11", licenses = ["notice"], sha256 = "72d402ed238019b638aefb3b592ddde9c52cfbb7956aadcbd419b8c76febc1b1")
    _maven_import(artifact = "org.apache.kafka:kafka-clients:1.0.0", licenses = ["notice"], sha256 = "8644fba65a277c41831c8704c6cb49b146c8278f836abd8c2d04dbffdc1a7c4a")
    _maven_import(artifact = "com.google.guava:guava:18.0", licenses = ["notice"], sha256 = "d664fbfc03d2e5ce9cab2a44fb01f1d0bf9dfebeccc1a473b1f9ea31f79f6f99")
    _maven_import(artifact = "com.puppycrawl.tools:checkstyle:6.17", licenses = ["notice"], sha256 = "61a8b52d03a5b163d0983cdc4b03396a92ea7f8dc8c007dda30f4db673e9e60c")
    _maven_import(artifact = "org.apache.htrace:htrace-core4:4.2.0-incubating", licenses = ["notice"], sha256 = "fcab21b4ae0829e99142d77240fa2963a85f6ff1ca3fc0f386f8b4ff3cae3b82")
    _maven_import(artifact = "org.apache.hadoop:hadoop-mapreduce-client-core:2.9.0", licenses = ["notice"], sha256 = "5582e4c08f218f0fef2765532279b5c7d9047a4599b29baf6b30e83760daa202")
    _maven_import(artifact = "com.fasterxml.jackson.core:jackson-databind:2.8.8", licenses = ["notice"], sha256 = "bd2959a21974cb361cea6a9295b6e8600e8b6a8c866a768d22b952016bce3248")
    _maven_import(artifact = "org.apache.curator:curator-client:4.0.0", licenses = ["notice"], sha256 = "c5c47e39c7756196c57eb89ffaf06877be42ce6e3478c89b4cb6df5cb4c250c9")
    _maven_import(artifact = "org.bouncycastle:bcprov-jdk15on:1.56", licenses = ["notice"], sha256 = "963e1ee14f808ffb99897d848ddcdb28fa91ddda867eb18d303e82728f878349")
    _maven_import(artifact = "org.bouncycastle:bcprov-jdk15on:1.56", licenses = ["notice"], sha256 = "963e1ee14f808ffb99897d848ddcdb28fa91ddda867eb18d303e82728f878349")
    _maven_import(artifact = "org.apache.hadoop:hadoop-common:2.9.0", licenses = ["notice"], sha256 = "b76d5d4372e659486eab13f7766541af2072b0ee81af752f6f7ce5b19773979f")
    _maven_import(artifact = "org.apache.hadoop:hadoop-hdfs-client:2.9.0", licenses = ["notice"], sha256 = "a7af83f48abfeee4dbb10ae0695d7f3a415f1a60d30a098e5f194f86fabb64f0")
    _maven_import(artifact = "org.apache.mesos:mesos:1.5.0", licenses = ["notice"], sha256 = "66cb1222778c0fd665d99bbd3b57e05e769fd5fe2683374fecc82dab50cf9376")
    _maven_import(artifact = "org.apache.hadoop:hadoop-hdfs:2.9.0", licenses = ["notice"], sha256 = "1cb5ed365bc486b802215d8b374a885d5743e760d733ed91948aa7ee6519cfd8")
    _maven_import(artifact = "io.kubernetes:client-java-proto:3.0.0", licenses = ["notice"], sha256 = "ed6b863c1cf141220c6c7e9160b2b139b8b1c18ec7334d713d451b7d804275a4")
    _maven_import(artifact = "io.kubernetes:client-java-api:3.0.0", licenses = ["notice"], sha256 = "22e51b86c3ea2421c0413be5b4c6978ee0a6f40532a79df4dcca0f4d3a02bfc0")
    _maven_import(artifact = "org.codehaus.woodstox:stax2-api:3.0.1", licenses = ["notice"], sha256 = "68ed11b72b138356063b8baf8551b9d67f46717b3c0001890949195c5b153199")
    _maven_import(artifact = "log4j:log4j:1.2.17", licenses = ["notice"], sha256 = "1d31696445697720527091754369082a6651bd49781b6005deb94e56753406f9")
    _maven_import(artifact = "it.unimi.dsi:fastutil:7.0.13", licenses = ["notice"], sha256 = "2ec909c77642b9c0220ab3e1c69bfcad3072789e2bcae5acdb5fb1df1ca14f04")
