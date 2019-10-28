install() {
  echo "Installing required tools for bazel..."
  sudo apt-get install pkg-config zip g++ zlib1g-dev unzip python3

  echo "Installing required tools for twister2..."
  sudo apt-get update
  sudo apt-get install g++ git build-essential automake cmake libtool-bin zip libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python-setuptools -y
  sudo apt-get install python3-dev python3-pip
  sudo pip3 install wheel
}

while true; do
  read -p "Do you wish to install system dependencies? This process will require sudo. [y/n]" yn
  case $yn in
  [Yy]*)
    install
    break
    ;;
  [Nn]*) exit ;;
  *) echo "Please answer yes or no." ;;
  esac
done
