---
id: Minikube
title: Minikube
sidebar_label: Minikube
---

We assume that you have installed Minikube on your machine and you can access the cluster with kubectl. 
Please start Minikube cluster if not already running. To install Twister2 on a regular Kubernetes cluster, 
please follow the steps at: [Installing Twister2 on Kubernetes](../kubernetes/twister2-install-kubernetes.md)

To install and compile Twister2 project on your machine, please follow the steps in [compiling document](../../compiling/compiling.md).
You may also check [developer document](../../developers/developer-environment.md) for setting up IDEs.

Here are the things that you need to do to run Twister2 jobs on Minikube:
* **Authorization of Pods**: This is a required step to run any Twister2 job on Kubernetes clusters. 
* **Deploying Twister2 Dashboard**: This is a recommended step to monitor Twister2 jobs on Kubernetes clusters. 
* **Persistent Storage Settings**: This is optional. You can set up persistent storage only if you want to use. 

**Requirement**: We have tested Twister2 on Minikube 1.5.0 with VirtualBox 6.0. 

## Authorization of Pods

Authorization of Pods is the same in Minikube and in regular Kubernetes clusters. 

If you are going to use the default namespace, then execute the following command: 

```bash
    $ kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-auth.yaml
```

If you are not going to use the default namespace, download the above yaml file, 
change the namespace field value to a namespace value that your users will use to submit Twister2 jobs. 
Then, execute the following command:

```bash
    $ kubectl create -f /path/to/file/twister2-auth.yaml
```

## Deploying Twister2 Dashboard

Twister2 Dashboard enables users to monitor their jobs through a web browser. 
Installing this dashboard is not mandatory for running Twister2 jobs. 
A single instance of the Dashboard is deployed to monitor all jobs.

If you are using default namespace, then you can deploy Twister Dashboard as:

```bash
    $ kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-dashboard-wo-ps.yaml
```

If you are using another namespace, or would like to change a parameter of Dashboard, 
then you can download this file, change the desired parameter value 
and execute the create command on the modified file. 

### Accessing Dashboard

You can access Twister2 Dashboard through your web browser.  

But, first you must start the proxy with the command: 

```bash
    $ kubectl proxy
```

Then, access Twister2 Dashboard at the following URL:

```text
    http://localhost:8001/api/v1/namespaces/default/services/http:twister2-dashboard:/proxy/#/
```

If you are using a namespace other than the default, change namespace value in above URL. 
Replace "default" with your namespace. 

If you are already running Kubernetes Dashboard with "minikube dashboard" command, 
it also starts a proxy. Therefore, instead of starting a separate proxy with "kubectl proxy", 
you can use that proxy. But, that proxy runs in another dynamically assigned port. 
You must replace the port number 8001 in the above URL with the port number of that proxy. 
You can get the port number from Kubernetes Dashboard url in your browser. 

## Persistent Storage Settings

Minikube comes with a default Persistent Storage Provisioner. 
Twister2 can use this provisioner to save job data. 

The storageClass name of this default provisioner is "standard". 
You need to update the value of following parameter in conf/kubernetes/resource.yaml as standard:

```text
    kubernetes.persistent.storage.class = "standard"
```

If you are using another Persistent Storage Provisioner, please set your storageClass name 
as the value of above parameter. 

Default Minikube provisioner creates a separate directory for each Twister2 job. 
Main directory for the provisioner under the VirtualBox machine is:

```text
    /tmp/hostpath-provisioner/
```

To check the content of this directory, first ssh into the virtualbox machine, 
then go to this directory:

```bash
    minikube ssh
    cd /tmp/hostpath-provisioner/
```

You will see a directory created for each running twister2 job. 
The directory name should be something like: pvc-1e2faca6-2c62-415a-a7ea-88db4b1caa8a

When the job completes and its resources deleted, this directory will also be deleted. 

## Modifying Twister2 Core Components

If you want to modify Twister2 core components, you need to build Twister2 Docker image. 
Please consult [Building Docker Image for Kubernetes](../kubernetes/buiding-docker-image.md)
