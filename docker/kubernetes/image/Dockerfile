FROM twister2/twister2-k8s-base:0.8.0-SNAPSHOT

# Disable prompts from apt.
ENV DEBIAN_FRONTEND noninteractive

###############################################################################
# Install Twister2 files and packages
#
ENV CLASSPATH="/twister2/lib/*"

COPY docker/kubernetes/image/rootfs /
COPY twister2-0.8.0-SNAPSHOT/lib /twister2/lib
COPY twister2-0.8.0-SNAPSHOT/bin /twister2/bin
COPY twister2-0.8.0-SNAPSHOT/conf/dashboard /twister2/conf/dashboard
COPY twister2-0.8.0-SNAPSHOT/conf/common /twister2/conf/common

# expose 2022 for ssh server (password free ssh support)
# expose 8080 for dashboard server if it runs
EXPOSE 2022 8080

WORKDIR /twister2/
