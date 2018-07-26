# Design Document for Twister2 OpenMPI Support in Kubernetes

#### Note 1: ssh port 
SSH port is hard coded inside the docker image. 
It is specified in Dockerfile and start_sshd.sh as 2022. 
We don't specify it in Kubernetes StatefulSet object, when we create the job. 
  