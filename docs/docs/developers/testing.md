---
id: tests
title: Unit Tests
sidebar_label: Unit Tests
---

For a developer, writing test cases is vital. In Twister2 development with bazel build, here is how
to setup a test package in your module. 

---
**NOTE**

The task API test bench is already created. So try it for a different module or your own module. 

---

Let's take an example of creating a test bench for task api. 

The package for this is twister2/task

## Step 1

Create a tests folder called test within the module that you want to create the 
tests. 

## Step 2

Within the tests folder create a folder called java. 

## Step 3 

Inside java folder, create a BUILD (a textfile) file

## Step 4

In the same level of BUILD file, create another folder with the expected package name
for including test classes.  



## Step 5 

Create any necessary sub packages and include java classes for tests. 

After completing Step 1 to 5, for instance the following folder structure will be an example 
output. 


```bash
twister2/task
├── src
└── tests
    └── java
        ├── BUILD
        └── edu.iu.dsc.tws.task
            └── test
                └── Hello.java
```

## Step 6

Adding content BUILD file. Depending on the module, pick the relevant dependencies to fit in the 
bazel build file and include these additional dependencies to support Junit testing. 

```bash
        "//third_party/java:junit4",
        "//third_party/java:mockito",
        "//third_party/java:powermock",
```

## Step 7

Create a test class called "Hello.java" (without quotes). Then add the test classes in the build
as follows. 

```java
package edu.iu.dsc.tws.task.test;

import org.junit.Test;

public class Hello {

  @Test
  public void hello() {
    System.out.println("Hello Task");
  }

}
```

```bash
java_tests(
    test_classes = [
        "edu.iu.dsc.tws.task.test.Hello",
    ],
    runtime_deps = [
        ":task-tests",
    ],
)
```

Overall BUILD file should take the following shape. (Dependencies can be dependable on what module 
you're working on)

```bash
load("//tools:rules/java_tests.bzl", "java_tests")

java_library(
    name = "task-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//third_party:ompi_javabinding_java",
        "//third_party/java:junit4",
        "//third_party/java:mockito",
        "//third_party/java:powermock",
        "//twister2/common/src/java:common-java",
        "//twister2/comms/src/java:comms-java",
        "//twister2/data/src/main/java:data-java",
        "@com_esotericsoftware_kryo//jar",
        "@com_google_guava_guava//jar",
        "@org_apache_commons_commons_lang3//jar",
        "@org_yaml_snakeyaml//jar",
    ],
)

java_tests(
    test_classes = [
        "edu.iu.dsc.tws.task.test.Hello",
    ],
    runtime_deps = [
        ":task-tests",
    ],
)
```

## Step 8

Set up this for the IDE. For supporting Intellij IDEA, you need to run the following script within
the twister2 cloned project. 

```bash
./scripts/setup-intellij.sh
```

This will create and make the test bench compatible with the IDE. After the initial setup, you 
can add test classes the same way you're doing it.

You're all set to do unit testing. 


## Running Unit Tests

As Bazel is our build tool, this is how to run the tests. 

You can specify your test class and run it quite easily as same as you do with Maven. 
For now IntelliJ doesn't have a inbuilt Bazel unit testing support.  

```bash
bazel test --config=ubuntu twister2/task/tests/java:Hello
```

It will give a sample output like this

```bash
INFO: Build completed successfully, 4 total actions
//twister2/task/tests/java:Hello                                         PASSED in 0.3s

Executed 1 out of 1 test: 1 test passes.
INFO: Build completed successfully, 4 total actions

```
  