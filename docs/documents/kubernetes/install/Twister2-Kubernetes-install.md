# Twister2 Installation in Kubernetes Clusters

## Authorization of Pods
Twister2 Worker pods need to get the IP address of the Job Master. 
In addition, Job Master needs to be able to delete used resources after 
the job has completed. Therefore, before running a Role and RoleBinding object need to be created. 
We prepared the following YAML file: twister2-auth.yaml.

First modify the namespace field in the twister2-auth.yaml. 
Change the value of this field to a namespace value, that users will use to submit Twister2 jobs.
Then execute the following command:

    $kubectl create -f twister2-auth.yaml

## Persistent Storage Settings
Twister2 expects that either a Persistent Storage Provisioner or statically configured 
PersistentVolume exists in the cluster. 
Persistent storage class needs to be specified in the client.yaml configuration file. 
Configuration parameter is: kubernetes.persistent.storage.class

We tested with NFS-Client provisioner from: 
https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client

NFS-Client provisioner is used if you already have an NFS server. 
Otherwise you may also use NFS provisioner 
that does not require to have an NFS provisioner: 
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

The fifth parameter is the name of the Secret object to be generated. 
That has to match the configuration parameter in the configuration files: 

    kubernetes.secret.name

You can retrieve the Secret object by executing in YAML form:

    $kubectl get secret <secret-name> -o=yaml

Another possibility for deploying the Secret object is to use the [YAML file template](../yaml-templates/secret.yaml). 
You can edit that secret.yaml file. You can put the public and private keys to the corresponding fields.
You can set the name and the namespace values. Then, you can create the Secret object by using
kubectl method as:

    $kubectl create secret -f /path/to/file/secret.yaml

