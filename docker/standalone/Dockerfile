FROM ubuntu:18.04


# install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-8-jdk sudo \
	python-dev python-pip g++ git build-essential \
	automake cmake libtool-bin zip libunwind-setjmp0-dev \
  zlib1g-dev unzip pkg-config python-setuptools python3-dev python3-pip \
  maven wget ssh && pip install wheel && pip3 install wheel && \
  rm -rf /var/lib/apt/lists/* && apt autoclean && apt-get clean && apt autoremove


# add user
RUN adduser twister2 && \
  echo "twister2 ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/twister2 && \
  chmod 0440 /etc/sudoers.d/twister2
USER twister2
WORKDIR /home/twister2


#install openmpi
RUN cd && \
	wget https://download.open-mpi.org/release/open-mpi/v4.0/openmpi-4.0.1.tar.gz && \
	tar -zxvf openmpi-4.0.1.tar.gz && \
	OMPI_BUILD=~/openmpi && \
	OMPI_401=~/openmpi-4.0.1 && \
	PATH=${OMPI_BUILD}/bin:${PATH} && \
	LD_LIBRARY_PATH=${OMPI_BUILD}/lib:${LD_LIBRARY_PATH} && \
	cd $OMPI_401 && \
	./configure --prefix=$OMPI_BUILD --enable-mpi-java --enable-mpirun-prefix-by-default --enable-orterun-prefix-by-default && make;make install && \
	echo "export OMPI_BUILD=$OMPI_BUILD" >> ~/.bashrc && \
	echo "export PATH=\${OMPI_BUILD}/bin:\${PATH}" >> ~/.bashrc && \
	echo "export LD_LIBRARY_PATH=\${OMPI_BUILD}/lib:\${LD_LIBRARY_PATH}" >> ~/.bashrc && \
	rm -rf ~/openmpi-4.0.1.tar.gz $OMPI_401


#install bazel
RUN wget https://github.com/bazelbuild/bazel/releases/download/1.1.0/bazel-1.1.0-installer-linux-x86_64.sh && \
	chmod +x bazel-1.1.0-installer-linux-x86_64.sh && \
	./bazel-1.1.0-installer-linux-x86_64.sh --user && \
	echo "export PATH=~/.bazel/bin:\$PATH" >> ~/.bashrc && \
	rm bazel-1.1.0-installer-linux-x86_64.sh


ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64



# to prevent cache use for git clone
ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache


# clone & build twister2
RUN git clone https://github.com/DSC-SPIDAL/twister2.git && \
	cd twister2 && git fetch && git checkout master && ~/.bazel/bin/bazel build --config=ubuntu scripts/package:tarpkgs && cd && \
	tar -xzvf twister2/bazel-bin/scripts/package/twister2-0.8.0-SNAPSHOT.tar.gz && \
	echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64" >> ~/.bashrc && \
	sed -i '/twister2.resource.scheduler.mpi.mpirun.file/c\twister2.resource.scheduler.mpi.mpirun.file: "mpirun"' /home/twister2/twister2-0.8.0-SNAPSHOT/conf/standalone/resource.yaml && \
	rm -rf bin ~/.cache/bazel
