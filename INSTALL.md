# Prerequisites

1. Operating System
  * Twister2 is tested and known to work on,
    *  Red Hat Enterprise Linux Server release 7
    *  Ubuntu 14.05
2. Java
  * Download Oracle JDK 8 from http://www.oracle.com/technetwork/java/javase/downloads/index.html
  * Extract the archive to a folder named `jdk1.8.0`
  * Set the following environment variables.
  ```
    JAVA_HOME=<path-to-jdk1.8.0-directory>
    PATH=$JAVA_HOME/bin:$PATH
    export JAVA_HOME PATH
  ```
3. OpenMPI
  * We recommend using `OpenMPI 3.0.0`

  * Download OpenMPI 3.0.0 from https://www.open-mpi.org/software/ompi/v3.0/downloads/openmpi-3.0.0.tar.gz
  * Extract the archive to a folder named `openmpi-3.0.0`
  * Also create a directory named `build` in some location. We will use this to install OpenMPI
  * Set the following environment variables
  ```
    BUILD=<path-to-build-directory>
    OMPI_300=<path-to-openmpi-3.0.0-directory>
    PATH=$BUILD/bin:$PATH
    LD_LIBRARY_PATH=$BUILD/lib:$LD_LIBRARY_PATH
    export BUILD OMPI_300 PATH LD_LIBRARY_PATH
  ```
  * The instructions to build OpenMPI depend on the platform. Therefore, we highly recommend looking into the `$OMPI_1101/INSTALL` file. Platform specific build files are available in `$OMPI_1101/contrib/platform` directory.
  * In general, please specify `--prefix=$BUILD` and `--enable-mpi-java` as arguments to `configure` script. If Infiniband is available (highly recommended) specify `--with-verbs=<path-to-verbs-installation>`. Usually, the path to verbs installation is `/usr`. In summary, the following commands will build OpenMPI for a Linux system.
  ```
    cd $OMPI_300
    ./configure --prefix=$BUILD --enable-mpi-java
    make;make install
  ```
  * If everything goes well `mpirun --version` will show `mpirun (Open MPI) 1.10.1`. Execute the following command to instal `$OMPI_300/ompi/mpi/java/java/mpi.jar` as a Maven artifact.
  ```
    mvn install:install-file -DcreateChecksum=true -Dpackaging=jar -Dfile=$OMPI_300/ompi/mpi/java/java/mpi.jar -DgroupId=ompi -DartifactId=ompijavabinding -Dversion=3.0.0
  ```
  * Few examples are available in `$OMPI_300/examples`. Please use `mpijavac` with other parameters similar to `javac` command to compile OpenMPI Java programs. Once compiled `mpirun [options] java -cp <classpath> class-name arguments` command with proper values set as arguments will run the MPI Java program.
4. Bazel build system
  * Twister2 requires Bazel build system version 0.30.
  * Download bazel from https://github.com/bazelbuild/bazel/releases/download/0.3.0/bazel-0.3.0-installer-linux-x86_64.sh
  * Execute the following commands

  ```
    chmod +x bazel-0.3.0-installer-linux-x86_64.sh
    ./bazel-0.3.0-installer-linux-x86_64.sh --user
    export PATH=$HOME/bin:$PATH
  ```
5. Required libraries

```
sudo apt-get install git build-essential automake cmake libtool-bin zip libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python-setuptools -y
```

``` 
sudo apt-get install  python-dev python-pip
```

# Compiling Twister2

The following commands can be used to compile Twister2

## Compiling the code

bazel build --config=ubuntu twister2/...

## Building the packages

bazel build --config=ubuntu //scripts/package:tarpkgs

The packages are installed at the following location

```
bazel-bin/scripts/package
```

You can exctract the `bazel-bin/scripts/package/twister2-client.tar.gz` to run Twister2.

If you get errors make sure you have the pre-requisites installed

```
sudo apt-get install git build-essential automake cmake libtool-bin zip libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python-setuptools -y

sudo apt-get install  python-dev python-pip
```


