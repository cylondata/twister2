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
# Install OpenJDK-11
# from: https://github.com/AdoptOpenJDK/openjdk-docker/blob/master/11/jdk/ubuntu/Dockerfile.hotspot.releases.full
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates fontconfig locales \
    && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_VERSION jdk-11.0.6+10

RUN set -eux; \
    ARCH="$(dpkg --print-architecture)"; \
    case "${ARCH}" in \
       aarch64|arm64) \
         ESUM='04b77f6754aed68528f39750c5cfd6a439190206aff216aa081d62a0e1a794fa'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.6%2B10/OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.6_10.tar.gz'; \
         ;; \
       armhf|armv7l) \
         ESUM='ab5b76203e54fe7a5221535f6f407efa43153de029a746f60af3cffb7cb5080b'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.6%2B10/OpenJDK11U-jdk_arm_linux_hotspot_11.0.6_10.tar.gz'; \
         ;; \
       ppc64el|ppc64le) \
         ESUM='9247f0271744188489b0dd628cab90e76ca1f22fa3bbcdebd9bfc4f140908df5'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.6%2B10/OpenJDK11U-jdk_ppc64le_linux_hotspot_11.0.6_10.tar.gz'; \
         ;; \
       s390x) \
         ESUM='250fc79db2d6c70e655ff319e2db8ca205858bf82c9f30b040bda0c90cd9f583'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.6%2B10/OpenJDK11U-jdk_s390x_linux_hotspot_11.0.6_10.tar.gz'; \
         ;; \
       amd64|x86_64) \
         ESUM='330d19a2eaa07ed02757d7a785a77bab49f5ee710ea03b4ee2fa220ddd0feffc'; \
         BINARY_URL='https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.6%2B10/OpenJDK11U-jdk_x64_linux_hotspot_11.0.6_10.tar.gz'; \
         ;; \
       *) \
         echo "Unsupported arch: ${ARCH}"; \
         exit 1; \
         ;; \
    esac; \
    curl -LfsSo /tmp/openjdk.tar.gz ${BINARY_URL}; \
    echo "${ESUM} */tmp/openjdk.tar.gz" | sha256sum -c -; \
    mkdir -p /opt/java/openjdk; \
    cd /opt/java/openjdk; \
    tar -xf /tmp/openjdk.tar.gz --strip-components=1; \
    rm -rf /tmp/openjdk.tar.gz;

ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:$PATH"

###############################################################################
# Install SSH and OpenMPI
#
# ssh and MPI installation is based on the Dockerfile from
#    https://github.com/everpeace

ARG OPENMPI_VERSION="4.0.2"
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
    apt-get install -y python3 nano && \
    rm -rf /var/lib/apt/lists/*

