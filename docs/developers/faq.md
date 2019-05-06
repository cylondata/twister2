# FAQ

## How can I configure my IDE to work with Twister2

After cloning Twister2 source to your workstation, run crips/setup-intellij.sh if you are using IntelliJ or scripts/setup-eclispe.sh if you are using Eclipse. This will generate IDE specific project metadata files in the root directory of the project.

## How can I add a new maven dependency to the project

Specify your library in WORKSPACE file using maven\_jar rule.

`maven_jar( name = "my_new_dependency", artifact = "com.my.dep:my-lib:0.2.1" )`

Specify your newly added dependency in relevant BUILD file as follows, to associate it with java library that you are working on.

`java_library( name = "my-library", srcs = glob(["**/*.java"]), deps = ["@my_new_dependency//jar"], )`

To make suggestions available within your IDE, clean project metadata files\(.iml or .project\) and re-run scrips/setup-intellij.sh if you are using IntelliJ or scripts/setup-eclispe.sh if you are using Eclipse.

