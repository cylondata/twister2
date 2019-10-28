# check java home
exists() {
  command -v "$1" >/dev/null 2>&1
}

echo "Looking for java..."
if ! exists $JAVA_HOME/bin/java; then
  echo "Error: Couldn't find java at JAVA_HOME. Please set JAVA_HOME properly." >&2
  exit 1
else
  echo "Found java at $JAVA_HOME"
fi

# install system dependencies
if [ "$(
  . /etc/os-release
  echo $NAME
)" = "Ubuntu" ]; then
  sh install_sys_dep_ubuntu.sh
else
  echo "This script supports only dependency installation for Ubuntu. Please manually install dependencies based on following URLS".
  echo "https://docs.bazel.build/versions/master/install.html"
  echo "https://twister2.org/docs/compiling/linux"
fi

echo "Looking for bazel..."
if ! exists bazel; then
  echo "Bazel not found on this system. Installing bazel..." >&2
  wget https://github.com/bazelbuild/bazel/releases/download/1.1.0/bazel-1.1.0-installer-linux-x86_64.sh -O /tmp/bazel.sh
  chmod 700 /tmp/bazel.sh
  /tmp/bazel.sh --user
  export PATH=$HOME/bin:$PATH
else
  echo "Found bazel at $(command -v bazel)"
fi

echo "Building twister2..."
bazel build --config=ubuntu scripts/package:tarpkgs
