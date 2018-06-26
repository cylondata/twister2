Twister2 Installation in Kubernetes Clusters

Authorization of Pods
Worker pods need to get the IP address of the Job Master. 
In addition, Job Master needs to be able to delete used resources after 
the job has completed. Therefore, before running Twister2 jobs, 
twister2-auth.yaml file needs to be created on Kubernetes master:
$kubectl create -f twister2-auth.yaml

Persistent Storage Settings
Twister2 expects that a Persistent Storage Provisioner exists in the cluster. 
Persistent storage class needs to be specified in the client.yaml configuration file. 
We tested with NFS-Client provisioner from: 
https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client

NFS-Client provisioner is used if you already have an NFS server. 
Otherwise you may also use NFS provisioner 
that does not require to have an NFS provisioner: 
https://github.com/kubernetes-incubator/external-storage/tree/master/nfs

Before proceeding with Twister2, make sure your persistent storage provisioner 
is working properly. 
