# Compiling on MacOS

## MacOS Prerequisites

Twister2 build needs several software installed on your system.

1. Operating System
   * Twister2 is tested and known to work on,
     * MacOS High Sierra (10.13.6)

2. Java
   * Download Oracle JDK 8 from [http://www.oracle.com/technetwork/java/javase/downloads/index.html](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
   * Install Oracle JDK 8 using jdk-8uxxx-macosx-x64.dmg
   * Set the following environment variables.

      ```bash
     JAVA_HOME=$(/usr/libexec/java_home)
     export JAVA_HOME
     ```
3. Install Homebrew
   
```bash
   /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```


4. Installing maven :

```bash
  brew install maven
```

5. Install bazel 0.18.1

   ```bash
      wget https://github.com/bazelbuild/bazel/releases/download/0.18.1/bazel-0.18.1-installer-darwin-x86_64.sh
      chmod +x bazel-0.18.1-installer-darwin-x86_64.sh
      ./bazel-0.18.1-installer-darwin-x86_64.sh –user
   ```

   Make sure to add the bazel bin to PATH

   ```text
   export PATH="$PATH:$HOME/bin"
   ```

## Compiling Twister2 on MacOS

Now lets get a clone of the source code.

```bash
git clone https://github.com/DSC-SPIDAL/twister2.git
```

You can compile the Twister2 distribution by using the bazel target as below.

```bash
cd twister2
bazel build --config=darwin scripts/package:tarpkgs
```

This will build twister2 distribution in the file

```bash
bazel-bin/scripts/package/twister2-0.2.1.tar.gz
```

If you would like to compile the twister2 without building the distribution packages use the command

```bash
bazel build --config=darwin twister2/...
```

For compiling a specific target such as communications

```bash
bazel build --config=darwin twister2/comms/src/java:comms-java
```

## Twister2 Distribution

After you've build the Twister2 distribution, you can extract it and use it to submit jobs.

```bash
cd bazel-bin/scripts/package/
tar -xvf twister2-0.2.1.tar.gz
```