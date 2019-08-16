FROM ubuntu:16.04


# install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-8-jdk sudo  \
	python-dev python-pip g++ git build-essential  \
	automake cmake libtool-bin zip libunwind-setjmp0-dev \
    zlib1g-dev unzip pkg-config python-setuptools \
    maven wget  ssh && \
    rm -rf /var/lib/apt/lists/* && apt autoclean && apt-get clean && apt autoremove


# add user
RUN adduser twister2 && \
    echo "twister2 ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/twister2 && \
    chmod 0440 /etc/sudoers.d/twister2
USER twister2
WORKDIR /home/twister2


#install openmpi
RUN cd && \
	wget https://www.open-mpi.org/software/ompi/v3.1/downloads/openmpi-3.1.2.tar.gz && \
	tar -zxvf openmpi-3.1.2.tar.gz && \
	OMPI_BUILD=~/openmpi && \
	OMPI_312=~/openmpi-3.1.2 && \
	PATH=${OMPI_BUILD}/bin:${PATH} && \
	LD_LIBRARY_PATH=${OMPI_BUILD}/lib:${LD_LIBRARY_PATH} && \
	cd $OMPI_312 && \
	./configure --prefix=$OMPI_BUILD --enable-mpi-java --enable-mpirun-prefix-by-default --enable-orterun-prefix-by-default && make;make install && \
	echo "export OMPI_BUILD=$OMPI_BUILD" >> ~/.bashrc && \
	echo "export PATH=\${OMPI_BUILD}/bin:\${PATH}" >> ~/.bashrc && \
	echo "export LD_LIBRARY_PATH=\${OMPI_BUILD}/lib:\${LD_LIBRARY_PATH}" >> ~/.bashrc && \
	rm -rf ~/openmpi-3.1.2.tar.gz $OMPI_312


#install bazel
RUN wget https://github.com/bazelbuild/bazel/releases/download/0.18.1/bazel-0.18.1-installer-linux-x86_64.sh && \
	chmod +x bazel-0.18.1-installer-linux-x86_64.sh && \
	./bazel-0.18.1-installer-linux-x86_64.sh --user && \
	echo "export PATH=~/.bazel/bin:\$PATH" >> ~/.bashrc && \
	rm bazel-0.18.1-installer-linux-x86_64.sh


# clone & build twister2
RUN git clone https://github.com/DSC-SPIDAL/twister2.git && \
	cd twister2 && ~/.bazel/bin/bazel build --config=ubuntu scripts/package:tarpkgs && cd && \
	tar -xzvf twister2/bazel-bin/scripts/package/twister2-0.2.2.tar.gz && \
	echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64" >> ~/.bashrc && \
	sed -i '/twister2.resource.scheduler.mpi.mpirun.file/c\twister2.resource.scheduler.mpi.mpirun.file: "mpirun"' /home/twister2/twister2-0.2.2/conf/standalone/resource.yaml && \
	rm -rf bin ~/.cache/bazel
