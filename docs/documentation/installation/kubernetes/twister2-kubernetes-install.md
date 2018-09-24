Twister2 Installation in Kubernetes Clusters
============================================

## Authorization of Pods
Twister2 Worker pods need to get the IP address of the Job Master. 
In addition, Job Master needs to be able to delete used resources after the job has completed. 
Therefore, before submitting a job, a Role and a RoleBinding object need to be created. 
We prepared the following YAML file: twister2-auth.yaml.

First modify the namespace field in the twister2-auth.yaml. 
Change the value of this field to a namespace value, that users will use to submit Twister2 jobs.
Then execute the following command:

    $kubectl create -f twister2-auth.yaml

## Persistent Storage Settings
Twister2 expects that either a Persistent Storage Provisioner or statically configured 
PersistentVolume exists in the cluster. 
Persistent storage class needs to be specified in the client.yaml configuration file. 
Configuration parameter is: 

    kubernetes.persistent.storage.class

We used the default storage value as "twister2-nfs-storage". 
Please set your persistent storage class name in your provisioner and in the client.yaml config file. 

We tested with NFS-Client provisioner from: 
https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client

NFS-Client provisioner is used if you already have an NFS server. 
Otherwise you may use the NFS provisioner that does not require to have an NFS server: 
https://github.com/kubernetes-incubator/external-storage/tree/master/nfs

Before proceeding with Twister2, make sure you either setup a static PersistentVolume
or deployed a persistent storage provisioner.

## Generating Secret Object for OpenMPI Jobs
When using OpenMPI communications in Twister2, pods need to have password-free SSH access 
among them. This is accomplished by first generating an SSH key pair and 
deploying them as a Kubernetes Secret on the cluster. 

First, generate an SSH key pair by using:

    $ssh-keygen

Second, create a Kubernetes Secret object for the namespace of Twister2 users: 

    $kubectl create secret generic twister2-openmpi-ssh-key --from-file=id_rsa=/path/to/.ssh/id_rsa --from-file=id_rsa.pub=/path/to/.ssh/id_rsa.pub --from-file=authorized_keys=/path/to/.ssh/id_rsa.pub --namespace=default

The fifth parameter (twister2-openmpi-ssh-key) is the name of the Secret object to be generated. 
That has to match the following configuration parameter in the network.yaml file: 

    kubernetes.secret.name

You can retrieve the created Secret object in YAML form by executing the following command:

    $kubectl get secret <secret-name> -o=yaml

Another possibility for deploying the Secret object is to use the [YAML file template](../../architecture/resource-schedulers/kubernetes/yaml-templates/secret.yaml). 
You can edit that secret.yaml file. You can put the public and private keys to the corresponding fields.
You can set the name and the namespace values. Then, you can create the Secret object by using
kubectl method as:

    $kubectl create secret -f /path/to/file/secret.yaml

## Providing Rack and Datacenter information to Twister2
Twister2 can use rack names and data center names of the nodes when scheduling tasks. 
There are two ways administrators and user can provide this information. 

**Through Configuration Files**:  
Users can provide the IP addresses of nodes in racks in their clusters. 
In addition, they can provide the list of data centers with rack data in them. 

Here is an example configuration:

    kubernetes.datacenters.list:
    - dc1: ['blue-rack', 'green-rack']
    - dc2: ['rack01', 'rack02']

    kubernetes.racks.list:
    - blue-rack: ['node01.ip', 'node02.ip', 'node03.ip']
    - green-rack: ['node11.ip', 'node12.ip', 'node13.ip']
    - rack01: ['node51.ip', 'node52.ip', 'node53.ip']
    - rack02: ['node61.ip', 'node62.ip', 'node63.ip']

**Labelling Nodes With Rack and Data Center Information**:  
Administrators can label their nodes in the cluster for their rack and datacenter information. 
Each node in the cluster must be labelled once. 
When the user submits a Twister2 job, submitting client first queries Kubernetes master
for the labels of nodes. It provides this list to all workers in the job. 

**Note**: For this solution to work, job submitting users must have admin privileges. 

**Example Labelling Commands**: Administrators can use kubectl command to label the nodes
in the cluster. The format of the label creation command is as follows:

    >kubectl label node <node-name> <label-key>=<label-value>  

Then, used rack and data center labels must be provided in the configuration files. 
These configuration parameters are: 

    kubernetes.rack.labey.key
    kubernetes.datacenter.labey.key

**Access Method**: Users must specify which method they use to provide rack and datacenter data. 
They need to set the value of the configuration parameter: 

    kubernetes.node.locations.from.config

If the value of this parameter is true, 
Twister2 will try to get the rack and data center labels from the configuration files. 
Otherwise, it will try to get it from the Kubernetes master. 

## Running Uploader Web Server
