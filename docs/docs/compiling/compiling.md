---
id: compile_overview
title: Compile Overview
sidebar_label: Overview
---

Twister2 relies on the [Bazel build](https://bazel.build/) system. It is tested on Bazel [1.1](https://github.com/bazelbuild/bazel/releases/tag/1.1.0) version and it is recommended to use it for building.

Twister2 developers are mainly working on Ubuntu 16.04, Ubuntu 18.04, RHEL7 and MacOS. So it is recommended to use those platforms with the early versions and we would like to expand our system to different platforms in the future.

Refer the specific guides for details on different platforms.

* [Compiling on Linux](linux.md)
* [Compiling MacOS](macos.md)

### General Build Instructions

Twister2 can be compiled with the following command

```bash
bazel build --config=<platform> twister2/...
```

Here platform argument can be ubuntu or darwin (for macos).

In order to run twister2 jobs, we need to create a tar package. To create the tar package we can use
the following command.

```bash
bazel build --config=<platform> //scripts/package:tarpkgs
```

This command builds a tar file inside the directory with the current version.

```bash
bazel-bin/scripts/package/twister2-[verson].tar.gz
```

We need to extract this package to run a twister2 job.

### Running unit test cases

We can run twister2 test cases with the following command

```bash
bazel test --config=ubuntu twister2/...
```

In order to run a specific test we can use the following command, where we need to specify a target.

```bash
bazel test --config=ubuntu //twister2/comms/tests/java:BinaryTreeTest
```

### Compiling OpenMPI

When you compile Twister2 it will automatically download and compile OpenMPI 4.0.1 with it. If you don't like this version of OpenMPI and wants to use your own version, you can compile OpenMPI using following instructions.

* We recommend using `OpenMPI 4.0.1`
* Download OpenMPI 4.0.1 from [https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.1.tar.gz](https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.1.tar.gz)
* Extract the archive to a folder named `openmpi-4.0.1`
* Also create a directory named `build` in some location. We will use this to install OpenMPI
* Set the following environment variables

  ```text
  BUILD=<path-to-build-directory>
  OMPI_401=<path-to-openmpi-4.0.1-directory>
  PATH=$BUILD/bin:$PATH
  LD_LIBRARY_PATH=$BUILD/lib:$LD_LIBRARY_PATH
  export BUILD OMPI_401 PATH LD_LIBRARY_PATH
  ```

* The instructions to build OpenMPI depend on the platform. Therefore, we highly recommend looking into the `$OMPI_401/INSTALL` file. Platform specific build files are available in `$OMPI_401/contrib/platform` directory.
* In general, please specify `--prefix=$BUILD` and `--enable-mpi-java` as arguments to `configure` script. If Infiniband is available \(highly recommended\) specify `--with-verbs=<path-to-verbs-installation>`. Usually, the path to verbs installation is `/usr`. In summary, the following commands will build OpenMPI for a Linux system.

  ```text
  cd $OMPI_401
  ./configure --prefix=$BUILD --enable-mpi-java
  make -j 8;make install
  ```

* If everything goes well `mpirun --version` will show `mpirun (Open MPI) 4.0.1`. Execute the following command to instal `$OMPI_401/ompi/mpi/java/java/mpi.jar` as a Maven artifact.

 ```text
  mvn install:install-file -DcreateChecksum=true -Dpackaging=jar -Dfile=$OMPI_401/ompi/mpi/java/java/mpi.jar -DgroupId=ompi -DartifactId=ompijavabinding -Dversion=4.0.1
 ```
### Generating Maven Artifacts

Although twister2 uses bazel as its build system, it has an option to generate maven artifacts for each twister2 module.

To generate and install maven artifacts to your local repository, run following command at the root of twister2 source.

```./util/mvn/install-local-snapshot.sh```

For more details refer the section [Generate Maven Artifacts](generate-maven.md).

### Compiling Dashboard

Twister2 dashboard has two components.

1. Front End Web application; A [ReactJS](https://reactjs.org/) based web application
2. Web server; A [spring boot application](https://spring.io/projects/spring-boot), which exposes a RESTful Web Service (JAX-RS)

Even though (2) Embedded web server, has been included in the main bazel build, (1) Front End Web application should be build separately.

#### Prerequisites to build web application

* Node 6 or later : [Download](https://nodejs.org/en/download/)
* NPM 5.2 or later : [How to Setup](https://www.npmjs.com/get-npm)
* SASS : [How to setup](https://sass-lang.com/install)

#### Compiling web application

Having all above prerequisites ready, navigate to the root folder of dashboard client.

```cd dashboard/client```

#### Building SCSS

Now build SCSS files to produce CSS files with following command

```npm run build-css```

#### Building React app

Use below command to build the react application.

```npm run build```

This will create an optimized production build in ```{twister2_root}/dashboard/server/src/main/resources/static``` directory.

#### Building Dashboard

As the final step, [run the main twister2 build](compiling.md), to generate all the binaries including dashboard.


### FAQ

1. Build fails with ompi java binding errors

Try to do one of these things

Note: If you get an error while compiling or building Twister2 saying "Java bindings requested but no Java support found" please execute the following command to install Java using apt-get command

```bash
  sudo apt install openjdk-8-jdk
```

Or change line 385 of

```bash
third_party/ompi3/ompi.BUILD
```

From

```bash
'./configure --prefix=$$INSTALL_DIR --enable-mpi-java'
```

to

```bash
'./configure --prefix=$$INSTALL_DIR --enable-mpi-java --with-jdk-bindir=<path-to-jdk>/bin --with-jdk-headers=<path-to-jdk>/include',
```

Please replace 'path-to-jdk' with your jdk location.

