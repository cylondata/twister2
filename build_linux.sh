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

echo "Looking for bazel..."
if ! exists bazel; then
  echo "Bazel not found on this system. Installing bazel..." >&2
  sudo apt-get install pkg-config zip g++ zlib1g-dev unzip python3
  wget https://github.com/bazelbuild/bazel/releases/download/1.1.0/bazel-1.1.0-installer-linux-x86_64.sh -O /tmp/bazel.sh
  chmod 700 /tmp/bazel.sh
  /tmp/bazel.sh --user
  export PATH=$HOME/bin:$PATH
else
  echo "Found bazel at $(command -v bazel)"
fi

echo "Installing required tools..."
sudo apt-get update
sudo apt-get install g++ git build-essential automake cmake libtool-bin zip libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python-setuptools -y
sudo apt-get install python3-dev python3-pip
sudo pip3 install wheel

echo "Building twister2..."
bazel build --config=ubuntu scripts/package:tarpkgs
