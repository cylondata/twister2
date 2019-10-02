## Building Docker Image for Kubernetes

First build Twister2 project. 

This will generate the twister2 package file: 

bazel-bin/scripts/package/twister2-0.3.0.tar.gz

While on twister2 main directory, unpack this tar file:

```bash
$ tar xf bazel-bin/scripts/package/twister2-0.3.0.tar.gz
```

This will extract the files under twister2-0.3.0 directory. 

Build Docker image by running the following command from twister2 main directory. 
Update username with your Docker Hub user name. 

```bash
$ docker build -t <username>/twister2-k8s:0.3.0 -f docker/kubernetes/image/Dockerfile .
```

Push generated Docker image to Docker Hub. If you have not already logged in to Docker Hub account, 
please login first with command: "docker login docker.io" 
```bash
$ docker push <username>/twister2-k8s:0.3.0
```

Update Docker Image name in the following resource.yaml file. 
Put new Docker Image name as the value of the key: "twister2.resource.kubernetes.docker.image" 
```bash
$ nano twister2/config/src/yaml/conf/kubernetes/resource.yaml
```

You may also want to update the value of the following key to "Always" in the same "resource.yaml" 
file: 
````
kubernetes.image.pull.policy: "Always"
````
 
With this, docker will pull the twister2 image from Docker Hub at each run.  
This may slow down startup times for the jobs. 
However, if you repeatedly use the same docker image name and version,
This may be convenient.
  