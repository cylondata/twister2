# Compiling

Twister2 relies on the [Bazel build](https://bazel.build/) system to provide a flexible and fast build. Twister2 has been tested on Bazel 0.18.1 version and it is recommended to use it for building.

Twister2 developers are mainly working on Ubuntu 16.04, Ubuntu 18.04, and MacOS. So it is recommended to use those platforms with the early versions and we would like to expand our system to different platforms in the future.

Refer the specific guides for details on different platforms.

* [Compiling on Linux](linux.md)
* [Compiling MacOS](macos.md)

## Overview

Twister2 can be compiled with the following general command

```bash
bazel build --config=<platform> twister2/...
```

The platform can be ubuntu or darwin (for macos).

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

## Running unit test cases

We can run twister2 test cases with the following command

```bash
bazel test --config=ubuntu twister2/...
```

In order to run a specific test we can use the following command, where we need to specify a target.

```bash
bazel test --config=ubuntu //twister2/comms/tests/java:BinaryTreeTest
```

## Compiling OpenMPI

When you compile Twister2 it will automatically download and compile OpenMPI 3.1.2 with it. If you don't like this version of OpenMPI and wants to use your own version, you can compile OpenMPI using following instructions.

* We recommend using `OpenMPI 3.1.2`
* Download OpenMPI 3.0.0 from [https://download.open-mpi.org/release/open-mpi/v3.1/openmpi-3.1.2.tar.gz](https://download.open-mpi.org/release/open-mpi/v3.1/openmpi-3.1.2.tar.gz)
* Extract the archive to a folder named `openmpi-3.1.2`
* Also create a directory named `build` in some location. We will use this to install OpenMPI
* Set the following environment variables

  ```text
  BUILD=<path-to-build-directory>
  OMPI_312=<path-to-openmpi-3.1.2-directory>
  PATH=$BUILD/bin:$PATH
  LD_LIBRARY_PATH=$BUILD/lib:$LD_LIBRARY_PATH
  export BUILD OMPI_312 PATH LD_LIBRARY_PATH
  ```

* The instructions to build OpenMPI depend on the platform. Therefore, we highly recommend looking into the `$OMPI_1101/INSTALL` file. Platform specific build files are available in `$OMPI_1101/contrib/platform` directory.
* In general, please specify `--prefix=$BUILD` and `--enable-mpi-java` as arguments to `configure` script. If Infiniband is available \(highly recommended\) specify `--with-verbs=<path-to-verbs-installation>`. Usually, the path to verbs installation is `/usr`. In summary, the following commands will build OpenMPI for a Linux system.

  ```text
  cd $OMPI_312
  ./configure --prefix=$BUILD --enable-mpi-java
  make -j 8;make install
  ```

* If everything goes well `mpirun --version` will show `mpirun (Open MPI) 3.1.2`. Execute the following command to instal `$OMPI_312/ompi/mpi/java/java/mpi.jar` as a Maven artifact.

 ```text
  mvn install:install-file -DcreateChecksum=true -Dpackaging=jar -Dfile=$OMPI_312/ompi/mpi/java/java/mpi.jar -DgroupId=ompi -DartifactId=ompijavabinding -Dversion=3.1.2
 ```
## Generating Maven Artifacts

Although twister2 use bazel as its build system, it has an option to generate maven artifacts for each twister2 module.

To generate and install maven artifacts to your local repository, run following command at the root of twister2 source.

```./util/mvn/install-local-snapshot.sh```

For more details refer the section [Generate Maven Artifacts](generate-maven.md).

## FAQ

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

