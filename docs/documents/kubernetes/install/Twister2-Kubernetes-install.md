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
