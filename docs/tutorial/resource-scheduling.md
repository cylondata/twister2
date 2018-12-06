<img src="fox.png" width="150" height="150">

### Geoffrey C. FOX


# Resource Scheduling (Kubernetes, Mesos)


## Kubernetes Overview

Kubernetes is an open source system for managing containerized applications across multiple hosts, providing basic mechanisms for deployment, maintenance, and scaling of applications.

Kubernetes provides two types of computing components:

* pods
* containers

### Pods

Pods are like virtual machines. They run containers. The applications in containers perform the actual computation. A pod without a container is useless.

Pods have:

* a file system that can be shared by the running containers in the pod
* an IP address that can be used by the running containers in the pod

### Containers

Containers are packaged applications with all their dependencies. They are supposed to run without any outside dependency.

Containers run in pods in Kubernetes. There can be one or multiple containers running on a pod.

**Container to Container Communications**
Containers use the IP address of the encompassing pod to communicate with other containers. Containers have their own port numbers though.

Containers can communicate with other containers in the same pod with shared files or localhost network interface.

Containers can communicate with other containers in other pods using IP communication or mounting a shared files system among the pods such as NFS.

### Containers and Pod Management

Kubernetes provides services to manage containerized applications for deployment, maintenance, and scaling:

* It can restart failed containers.
* It can increase the number of instances running or decrease them \(scale up - scale down\).
* It can help rolling updates to applications.
* It provides monitoring tools for pods and containers.
* It provides networking facilities. It provides internal IP addresses to each pod.

  It can also expose pod applications to outside world by setting up services.

* It provides pod binding and mapping to nodes in clusters.

  Users can request their pods to run in some specific nodes or vice versa.


## Mesos Overview

Mesos is a fault tolerant cluster manager that provides resource
isolation and sharing across distributed applications or frameworks. It
schedules CPU and memory resources across the cluster in much the same
way the Linux Kernel schedules local resources. General Information
provided here about Mesos is taken from Apache Mesos web page.

The Mesos kernel runs on every machine and provides applications with
API's for resource management and scheduling across entire datacenter
and cloud environments. Mesos' two-level architecture allows it to run
existing and new distributed technologies on the same platform.  It can
scale up to 10000 nodes. It uses Zookeeper for fault tolerance.

There is a Mesos master and several Mesos agents. The master
enables fine-grained sharing of resources (CPU, RAM, ...) across
frameworks by making them resource offers. Each resource offer contains
a list of &lt;agent ID, resource1: amount1, resource2: amount2, ...&gt;.





<span style="color: green"> More content will be added soon.... </span>

