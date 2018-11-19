//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

let downloadAndProcess = require('./tw2_bzl');

let mvnDepTree = `[INFO] +- org.springframework.boot:spring-boot-starter:jar:2.1.0.RELEASE:compile
[INFO] |  +- org.springframework.boot:spring-boot:jar:2.1.0.RELEASE:compile
[INFO] |  |  \\- org.springframework:spring-context:jar:5.1.2.RELEASE:compile
[INFO] |  +- org.springframework.boot:spring-boot-autoconfigure:jar:2.1.0.RELEASE:compile
[INFO] |  +- org.springframework.boot:spring-boot-starter-logging:jar:2.1.0.RELEASE:compile
[INFO] |  |  +- ch.qos.logback:logback-classic:jar:1.2.3:compile
[INFO] |  |  |  \\- ch.qos.logback:logback-core:jar:1.2.3:compile
[INFO] |  |  +- org.apache.logging.log4j:log4j-to-slf4j:jar:2.11.1:compile
[INFO] |  |  \\- org.slf4j:jul-to-slf4j:jar:1.7.25:compile
[INFO] |  +- javax.annotation:javax.annotation-api:jar:1.3.2:compile
[INFO] |  +- org.springframework:spring-core:jar:5.1.2.RELEASE:compile
[INFO] |  |  \\- org.springframework:spring-jcl:jar:5.1.2.RELEASE:compile
[INFO] |  \\- org.yaml:snakeyaml:jar:1.23:runtime
[INFO] +- io.springfox:springfox-swagger2:jar:2.9.2:compile
[INFO] |  +- io.swagger:swagger-annotations:jar:1.5.20:compile
[INFO] |  +- io.swagger:swagger-models:jar:1.5.20:compile
[INFO] |  |  \\- com.fasterxml.jackson.core:jackson-annotations:jar:2.9.0:compile
[INFO] |  +- io.springfox:springfox-spi:jar:2.9.2:compile
[INFO] |  |  \\- io.springfox:springfox-core:jar:2.9.2:compile
[INFO] |  +- io.springfox:springfox-schema:jar:2.9.2:compile
[INFO] |  +- io.springfox:springfox-swagger-common:jar:2.9.2:compile
[INFO] |  +- io.springfox:springfox-spring-web:jar:2.9.2:compile
[INFO] |  +- com.google.guava:guava:jar:20.0:compile
[INFO] |  +- com.fasterxml:classmate:jar:1.4.0:compile
[INFO] |  +- org.slf4j:slf4j-api:jar:1.7.25:compile
[INFO] |  +- org.springframework.plugin:spring-plugin-core:jar:1.2.0.RELEASE:compile
[INFO] |  |  +- org.springframework:spring-beans:jar:5.1.2.RELEASE:compile
[INFO] |  |  \\- org.springframework:spring-aop:jar:5.1.2.RELEASE:compile
[INFO] |  +- org.springframework.plugin:spring-plugin-metadata:jar:1.2.0.RELEASE:compile
[INFO] |  \\- org.mapstruct:mapstruct:jar:1.2.0.Final:compile
[INFO] +- io.springfox:springfox-swagger-ui:jar:2.9.2:compile
[INFO] +- org.springframework.boot:spring-boot-starter-data-jpa:jar:2.1.0.RELEASE:compile
[INFO] |  +- org.springframework.boot:spring-boot-starter-aop:jar:2.1.0.RELEASE:compile
[INFO] |  |  \\- org.aspectj:aspectjweaver:jar:1.9.2:compile
[INFO] |  +- org.springframework.boot:spring-boot-starter-jdbc:jar:2.1.0.RELEASE:compile
[INFO] |  |  +- com.zaxxer:HikariCP:jar:3.2.0:compile
[INFO] |  |  \\- org.springframework:spring-jdbc:jar:5.1.2.RELEASE:compile
[INFO] |  +- javax.transaction:javax.transaction-api:jar:1.3:compile
[INFO] |  +- javax.xml.bind:jaxb-api:jar:2.3.1:compile
[INFO] |  |  \\- javax.activation:javax.activation-api:jar:1.2.0:compile
[INFO] |  +- org.hibernate:hibernate-core:jar:5.3.7.Final:compile
[INFO] |  |  +- org.jboss.logging:jboss-logging:jar:3.3.2.Final:compile
[INFO] |  |  +- javax.persistence:javax.persistence-api:jar:2.2:compile
[INFO] |  |  +- org.javassist:javassist:jar:3.23.1-GA:compile
[INFO] |  |  +- net.bytebuddy:byte-buddy:jar:1.9.3:compile
[INFO] |  |  +- antlr:antlr:jar:2.7.7:compile
[INFO] |  |  +- org.jboss:jandex:jar:2.0.5.Final:compile
[INFO] |  |  +- org.dom4j:dom4j:jar:2.1.1:compile
[INFO] |  |  \\- org.hibernate.common:hibernate-commons-annotations:jar:5.0.4.Final:compile
[INFO] |  +- org.springframework.data:spring-data-jpa:jar:2.1.2.RELEASE:compile
[INFO] |  |  +- org.springframework.data:spring-data-commons:jar:2.1.2.RELEASE:compile
[INFO] |  |  +- org.springframework:spring-orm:jar:5.1.2.RELEASE:compile
[INFO] |  |  \\- org.springframework:spring-tx:jar:5.1.2.RELEASE:compile
[INFO] |  \\- org.springframework:spring-aspects:jar:5.1.2.RELEASE:compile
[INFO] +- org.springframework.boot:spring-boot-starter-test:jar:2.1.0.RELEASE:test
[INFO] |  +- org.springframework.boot:spring-boot-test:jar:2.1.0.RELEASE:test
[INFO] |  +- org.springframework.boot:spring-boot-test-autoconfigure:jar:2.1.0.RELEASE:test
[INFO] |  +- com.jayway.jsonpath:json-path:jar:2.4.0:test
[INFO] |  |  \\- net.minidev:json-smart:jar:2.3:test
[INFO] |  |     \\- net.minidev:accessors-smart:jar:1.2:test
[INFO] |  |        \\- org.ow2.asm:asm:jar:5.0.4:test
[INFO] |  +- junit:junit:jar:4.12:test
[INFO] |  +- org.assertj:assertj-core:jar:3.11.1:test
[INFO] |  +- org.mockito:mockito-core:jar:2.23.0:test
[INFO] |  |  +- net.bytebuddy:byte-buddy-agent:jar:1.9.3:test
[INFO] |  |  \\- org.objenesis:objenesis:jar:2.6:test
[INFO] |  +- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] |  +- org.hamcrest:hamcrest-library:jar:1.3:test
[INFO] |  +- org.skyscreamer:jsonassert:jar:1.5.0:test
[INFO] |  |  \\- com.vaadin.external.google:android-json:jar:0.0.20131108.vaadin1:test
[INFO] |  +- org.springframework:spring-test:jar:5.1.2.RELEASE:test
[INFO] |  \\- org.xmlunit:xmlunit-core:jar:2.6.2:test
[INFO] +- org.springframework.boot:spring-boot-starter-web:jar:2.1.0.RELEASE:compile
[INFO] |  +- org.springframework.boot:spring-boot-starter-json:jar:2.1.0.RELEASE:compile
[INFO] |  |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.9.7:compile
[INFO] |  |  |  \\- com.fasterxml.jackson.core:jackson-core:jar:2.9.7:compile
[INFO] |  |  +- com.fasterxml.jackson.datatype:jackson-datatype-jdk8:jar:2.9.7:compile
[INFO] |  |  +- com.fasterxml.jackson.datatype:jackson-datatype-jsr310:jar:2.9.7:compile
[INFO] |  |  \\- com.fasterxml.jackson.module:jackson-module-parameter-names:jar:2.9.7:compile
[INFO] |  +- org.springframework.boot:spring-boot-starter-tomcat:jar:2.1.0.RELEASE:compile
[INFO] |  |  +- org.apache.tomcat.embed:tomcat-embed-core:jar:9.0.12:compile
[INFO] |  |  +- org.apache.tomcat.embed:tomcat-embed-el:jar:9.0.12:compile
[INFO] |  |  \\- org.apache.tomcat.embed:tomcat-embed-websocket:jar:9.0.12:compile
[INFO] |  +- org.hibernate.validator:hibernate-validator:jar:6.0.13.Final:compile
[INFO] |  |  \\- javax.validation:validation-api:jar:2.0.1.Final:compile
[INFO] |  +- org.springframework:spring-web:jar:5.1.2.RELEASE:compile
[INFO] |  \\- org.springframework:spring-webmvc:jar:5.1.2.RELEASE:compile
[INFO] |     \\- org.springframework:spring-expression:jar:5.1.2.RELEASE:compile
[INFO] +- com.h2database:h2:jar:1.4.197:runtime
[INFO] +- org.apache.logging.log4j:log4j-api:jar:2.11.1:compile
[INFO] +- org.apache.logging.log4j:log4j-core:jar:2.11.1:compile
[INFO] \\- org.apache.logging.log4j:log4j-slf4j-impl:jar:2.11.1:compile`;

let regex = new RegExp("[a-z\\.0-9]+:[a-z\\.0-9\\-]+:jar:[a-z\\.0-9A-Z]+", "g");

let groups = regex.exec(mvnDepTree);
while (groups) {
    //console.log(groups[0]);
    //downloadAndProcess(groups[0].replace(":jar",""))
    let parts = groups[0].replace(":jar", "").split(":");
    console.log('"@' + (parts[0] + "_" + parts[1]).replace(/[:\.-]/g, "_") + '//jar",);
    groups = regex.exec(mvnDepTree);
}
