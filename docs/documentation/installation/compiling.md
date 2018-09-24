# Compiling Twister2

Twister2 relies on the [Bazel build](https://bazel.build/) system to provide a flexible and
fast build. Twister2 has been tested on Bazel 0.8.1 version and it
is recommended to use it for building.

Twister2 developers are mainly working on Ubuntu 16.04 and Ubuntu 18.04.
So it is recommended to use those platforms with the early versions and
we would like to expand our system to different platforms in the future.

## Prerequisites

Twister2 build needs several software installed on your system.

1. Operating System
  * Twister2 is tested and known to work on,
    *  Red Hat Enterprise Linux Server release 7
    *  Ubuntu 14.05, Ubuntu 16.10 and Ubuntu 18.10
2. Java
  * Download Oracle JDK 8 from http://www.oracle.com/technetwork/java/javase/downloads/index.html
  * Extract the archive to a folder named `jdk1.8.0`
  * Set the following environment variables.
  ```
    JAVA_HOME=<path-to-jdk1.8.0-directory>
    PATH=$JAVA_HOME/bin:$PATH
    export JAVA_HOME PATH
  ```
3. Install the required tools

```bash
   sudo apt-get install g++ git build-essential automake cmake libtool-bin zip libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python-setuptools -y
```

```
sudo apt-get install  python-dev python-pip
```

4. Installing maven and configure it as follows :

```
  wget http://mirrors.ibiblio.org/apache/maven/maven-3/3.5.2/binaries/apache-maven-3.5.2-bin.tar.gz
```
  Extract this to a directory called maven configure the environmental variables
```
  MAVEN_HOME=<path-to-maven-directory>
  PATH=$MAVEN_HOME/bin:$PATH
  export MAVEN_HOME PATH
```

5. Install bazel 0.8.1

   ```bash

   wget https://github.com/bazelbuild/bazel/releases/download/0.8.1/bazel-0.8.1-installer-linux-x86_64.sh
   chmod +x bazel-0.8.1-installer-linux-x86_64.sh
   ./bazel-0.8.1-installer-linux-x86_64.sh --user
   ```

   Make sure to add the bazel bin to PATH

   ```bath
   export PATH=$PATH:~/bin
   ```

## Compiling Twister2

Now lets get a clone of the source code.

```bash

git clone https://github.com/DSC-SPIDAL/twister2.git

```


You can compile the Twister2 distribution by using the bazel target as
below.

```bash

cd twister2
bazel build --config=ubuntu scripts/package:tarpkgs

```

This will build twister2 distribution in the file

```bash
bazel-bin/scripts/package/twister2-client.tar.gz
```

If you would like to compile the twister2 without building the distribution
packages use the command

```bash

bazel build --config=ubuntu twister2/...

```

For compiling a specific target such as communications

```bash

bazel build --config=ubuntu twister2/comms/src/java:comms-java

```

## Twister2 Distribution

After you've build the Twister2 distribution, you can extract it and use
it to submit jobs.

```bash
cd bazel-bin/scripts/package/
tar -xvf twister2-client.tar.gz
```

## Compiling OpenMPI

When you compile Twister2 it will automatically download and compile OpenMPI 3.1.2 with it.
If you don't like this version of OpenMPI and wants to use your own version,
you can compile OpenMPI using following instructions.

  * We recommend using `OpenMPI 3.1.2`
  * Download OpenMPI 3.0.0 from https://download.open-mpi.org/release/open-mpi/v3.1/openmpi-3.1.2.tar.gz
  * Extract the archive to a folder named `openmpi-3.1.2`
  * Also create a directory named `build` in some location. We will use this to install OpenMPI
  * Set the following environment variables
  ```
    BUILD=<path-to-build-directory>
    OMPI_312=<path-to-openmpi-3.1.2-directory>
    PATH=$BUILD/bin:$PATH
    LD_LIBRARY_PATH=$BUILD/lib:$LD_LIBRARY_PATH
    export BUILD OMPI_312 PATH LD_LIBRARY_PATH
  ```
  * The instructions to build OpenMPI depend on the platform. Therefore, we highly recommend looking into the `$OMPI_1101/INSTALL` file. Platform specific build files are available in `$OMPI_1101/contrib/platform` directory.
  * In general, please specify `--prefix=$BUILD` and `--enable-mpi-java` as arguments to `configure` script. If Infiniband is available (highly recommended) specify `--with-verbs=<path-to-verbs-installation>`. Usually, the path to verbs installation is `/usr`. In summary, the following commands will build OpenMPI for a Linux system.
  ```
    cd $OMPI_312
    ./configure --prefix=$BUILD --enable-mpi-java
    make -j 8;make install
  ```
  * If everything goes well `mpirun --version` will show `mpirun (Open MPI) 3.1.2`. Execute the following command to instal `$OMPI_312/ompi/mpi/java/java/mpi.jar` as a Maven artifact.
  ```
    mvn install:install-file -DcreateChecksum=true -Dpackaging=jar -Dfile=$OMPI_312/ompi/mpi/java/java/mpi.jar -DgroupId=ompi -DartifactId=ompijavabinding -Dversion=3.1.2
  ```