## Building Docker Image for Kubernetes

First build Twister2 project. 

This will generate the twister2 package file: 

bazel-bin/scripts/package/twister2-0.1.0.tar.gz

While on twister2 main directory, unpack this tar file:

```bash
$ tar xf bazel-bin/scripts/package/twister2-0.1.0.tar.gz
```

This will extract the files under twister2-0.1.0 directory. 

Build Docker image by running the following command from twister2 main directory. 
Update username with your user name. 

```bash
$ docker build -t username/twister2-k8s:0.1.0 -f docker/kubernetes/image/Dockerfile .
```

Push generated Docker image to Docker Hub
```bash
$ docker push username/twister2-k8s:0.1.0
```
