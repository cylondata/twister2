---
id: generate-maven
title: Maven Artifacts
sidebar_label: Maven Artifacts
---

At this point bazel doesnt provide a method to generate maven artifacts directly. We have created 
set of scripts to generate the maven artifacts.

You can generate the maven artifacts using the following command from the twister2 source root directory.

You need to compile Twister2 before executing this command.

```bash
sh util/mvn/install-local-snapshot.sh
```

This will produce the maven artifacts. We can change the version of the artifacts generated in the 
`t2_meta.bzl` file in the root of Twister2 source. `T2_VERSION = "0.4.0"`.

[Twister2 Applications](https://github.com/DSC-SPIDAL/twister2applications) is a repository 
containing a set of examples and applications written as maven modules which exploit twister2 maven artifacts.

Currently it produce the following maven dependencies.

```xml
  <dependencies>
    <dependency>
      <groupId>org.twister2</groupId>
      <artifactId>comms-java</artifactId>
      <version>0.4.0</version>
    </dependency>
    <dependency>
      <groupId>org.twister2</groupId>
      <artifactId>proto-java</artifactId>
      <version>0.4.0</version>
    </dependency>
    <dependency>
      <groupId>org.twister2</groupId>
      <artifactId>resource-scheduler-java</artifactId>
      <version>0.4.0</version>
    </dependency>
    <dependency>
      <groupId>org.twister2</groupId>
      <artifactId>common-java</artifactId>
      <version>0.4.0</version>
    </dependency>
    <dependency>
      <groupId>org.twister2</groupId>
      <artifactId>api-java</artifactId>
      <version>0.4.0</version>
    </dependency>
    <dependency>
      <groupId>org.twister2</groupId>
      <artifactId>data-java</artifactId>
      <version>0.4.0</version>
    </dependency>
    <dependency>
      <groupId>org.twister2</groupId>
      <artifactId>task-java</artifactId>
      <version>0.4.0</version>
    </dependency>
    <dependency>
      <groupId>org.twister2</groupId>
      <artifactId>taskscheduler-java</artifactId>
      <version>0.4.0</version>
    </dependency>
  </dependencies>
```


