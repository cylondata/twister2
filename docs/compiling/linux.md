# Compiling on Linux

## Linux Prerequisites

Twister2 build needs several software installed on your system.

1. Operating System
   * Twister2 is tested and known to work on,
     * Red Hat Enterprise Linux Server release 7
     * Ubuntu 14.05, Ubuntu 16.10 and Ubuntu 18.10
2. Java
   * Download Oracle JDK 8 from [http://www.oracle.com/technetwork/java/javase/downloads/index.html](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
   * Extract the archive to a folder named `jdk1.8.0`
   * Set the following environment variables.

     ```text
     JAVA_HOME=<path-to-jdk1.8.0-directory>
     PATH=$JAVA_HOME/bin:$PATH
     export JAVA_HOME PATH
     ```
3. Install the required tools

```bash
   sudo apt-get install g++ git build-essential automake cmake libtool-bin zip libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python-setuptools -y
```

```text
sudo apt-get install  python-dev python-pip
```

4. Installing maven and configure it as follows :

```text
  wget http://mirrors.ibiblio.org/apache/maven/maven-3/3.5.2/binaries/apache-maven-3.5.2-bin.tar.gz
```

Extract this to a directory called maven configure the environmental variables

```text
  MAVEN_HOME=<path-to-maven-directory>
  PATH=$MAVEN_HOME/bin:$PATH
  export MAVEN_HOME PATH
```

5. Install bazel 0.18.1

   ```bash
   wget https://github.com/bazelbuild/bazel/releases/download/0.18.1/bazel-0.18.1-installer-linux-x86_64.sh
   chmod +x bazel-0.18.1-installer-linux-x86_64.sh
   ./bazel-0.18.1-installer-linux-x86_64.sh --user
   ```

   Make sure to add the bazel bin to PATH

   ```text
   export PATH=$PATH:~/bin
   ```

## Compiling Twister2 on Linux

Now lets get a clone of the source code.

```bash
git clone https://github.com/DSC-SPIDAL/twister2.git
```

You can compile the Twister2 distribution by using the bazel target as below.

```bash
cd twister2
bazel build --config=ubuntu scripts/package:tarpkgs
```

This will build twister2 distribution in the file

```bash
bazel-bin/scripts/package/twister2-0.2.1.tar.gz
```

If you would like to compile the twister2 without building the distribution packages use the command

```bash
bazel build --config=ubuntu twister2/...
```

For compiling a specific target such as communications

```bash
bazel build --config=ubuntu twister2/comms/src/java:comms-java
```

## Twister2 Distribution

After you've build the Twister2 distribution, you can extract it and use it to submit jobs.

```bash
cd bazel-bin/scripts/package/
tar -xvf twister2-0.2.1.tar.gz
```