FROM ubuntu:16.04

# Disable prompts from apt.
ENV DEBIAN_FRONTEND noninteractive

##############################################################################
# install apt-utils and locales, set up language
RUN apt-get update && apt-get install -y apt-utils locales locales-all
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8

##############################################################################
# Install Java 8.
# Based on:
#    https://github.com/dockerfile/java/blob/master/oracle-java8/Dockerfile
#    https://stackoverflow.com/questions/49914574/install-jdk-8-update-172-in-dockerfile-with-ubuntu-image
#

RUN \
   apt-get update && \
   apt-get -y install software-properties-common && \
   echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
   add-apt-repository -y ppa:webupd8team/java && \
   apt-get update && \
   apt-get install -y oracle-java8-installer && \
   rm -rf /var/cache/oracle-jdk8-installer

# Define JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

###############################################################################
# Install SSH and OpenMPI
#
# ssh and MPI installation is based on the Dockerfile from
#    https://github.com/everpeace

ARG OPENMPI_VERSION="3.1.2"
ARG WITH_CUDA="false"

#
# install ssh and basic dependencies
#
RUN apt-get update && \
    apt-get install -yq --no-install-recommends \
      wget ca-certificates ssh build-essential && \
    rm -rf /var/cache/apt/archives/*

#
# install openmpi
#
RUN cd /tmp && \
  echo "WITH_CUDA=$WITH_CUDA" && \
  wget -q https://www.open-mpi.org/software/ompi/v$(echo $OPENMPI_VERSION | sed -e s'/\(.*\)\.[0-9]/\1/')/downloads/openmpi-$OPENMPI_VERSION.tar.bz2 && \
  tar -xjf openmpi-$OPENMPI_VERSION.tar.bz2 && \
  cd /tmp/openmpi-$OPENMPI_VERSION && \
  if [ "$WITH_CUDA" = "true" ]; then export WITH_CUDA_OPT="--with-cuda"; else export WITH_CUDA_OPT=""; fi && \
  echo "WITH_CUDA_OPT=$WITH_CUDA_OPT" && \
  ./configure --prefix=/usr --enable-mpi-java $WITH_CUDA_OPT && \
  make -j2 && \
  make install && \
  rm -rf /tmp/openmpi-$OPENMPI_VERSION /tmp/openmpi-$OPENMPI_VERSION.tar.bz2

# set LD_LIBRARY_PATH environment variable
ENV LD_LIBRARY_PATH   /usr/lib

#
# Create ssh user(openmpi) and setup ssh key dir
# - ssh identity file and authorized key file is expected to
#   be mounted at /ssh-keys/$SSH_USER
#
ARG SSH_USER=openmpi
ENV SSH_USER=$SSH_USER
ARG SSH_UID=1000
ARG SSH_GID=1000
VOLUME /ssh-key/$SSH_USER
ARG HOME=/home/$SSH_USER

RUN addgroup --gid $SSH_GID $SSH_USER && \
    adduser -q --gecos "" --disabled-password --uid $SSH_UID --gid $SSH_GID $SSH_USER && \
    mkdir -p /ssh-key/$SSH_USER && \
       chown -R $SSH_USER:$SSH_USER /ssh-key/$SSH_USER && \
    mkdir -p /.sshd/host_keys && \
       chown -R $SSH_USER:$SSH_USER /.sshd/host_keys && \
       chmod 700 /.sshd/host_keys && \
    mkdir -p /.sshd/user_keys/$SSH_USER && \
       chown -R $SSH_USER:$SSH_USER /.sshd/user_keys/$SSH_USER && \
       chmod 700 /.sshd/user_keys/$SSH_USER  && \
    mkdir -p $HOME && \
       chown $SSH_USER:$SSH_USER $HOME && \
       chmod 755 $HOME

VOLUME $HOME

# check if open mpi was successfully built with cuda support.
RUN if [ "$WITH_CUDA" = "true" ]; then \
  if ! ompi_info --parsable --all | grep -q "mpi_built_with_cuda_support:value:true" ; then \
    exit 1; \
  fi; fi;

###############################################################################
# install python
# clear apt-get cache, no more apt usage after this point
RUN apt-get update && \
    apt-get install -y python && \
    rm -rf /var/lib/apt/lists/*

###############################################################################
# Install Twister2 files and packages
#
ENV CLASSPATH="/twister2/lib/*"

COPY docker/kubernetes/image/rootfs /
COPY twister2-0.2.1/lib /twister2/lib
COPY twister2-0.2.1/bin /twister2/bin

# expose 2022 for ssh server (password free ssh support)
# expose 8080 for dashboard server if it runs
EXPOSE 2022 8080

WORKDIR /twister2/
