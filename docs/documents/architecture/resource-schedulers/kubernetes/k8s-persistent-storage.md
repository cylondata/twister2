Persistent Storage on Kubernetes
================================

Kubernetes provides [persistent storage](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) 
support by using any one of the supported distributed file systems. 
Some of the supported file systems are: gcePersistentDisk, awsElasticBlockStore, Cinder, 
glusterfs, rbd, Azure File, Azure Disk, Portworx, NFS.
  
We tested Twister2 persistent storage support with Network File System (NFS). 
Other file systems should also work. We installed an NFS server in one of the cluster servers.
We also deployed the [NFS-Client provisioner](https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client). 
It handles dynamic PersistentVolume creation and deletions. 
NFS-Client provisioner runs as a separate deployment in the cluster.

We create a separate PersistentVolumeClaim object for each job.  
PersistentVolume creation is handled dynamically by the provisioner. 
A separate persistent directory is created for each job by the provisioner in our NFS server. 

Twister2 should also work with static PersistentVolumes. 
However, it is much more convenient to use a dynamic provisioner. 

Each twister2 pod attaches itself to the shared NFS directory. 
Each pod mounts to the persistent volume as: 

    /persistent 

Since the provisioner provides a separate directory for each PersistentVolume,
we do not create a separate job directory under this directory. 
A separate logs directory is also created for persistent log files. 

## Configuration Settings
**Persistent Storage Class**: Cluster administrators provide a StorageClass object 
to dynamically create PersistentVolumes. Its name must be provided as a parameter 
in the configuration files. Parameter name:
 
     kubernetes.persistent.storage.class

**Persistent Volume Sizes**: A parameter determines the size of the persistent volume 
on each worker. It is called: 

    persistent.volume.per.worker

When persistent.volume.per.worker value is zero, twister2 does not initialize persistent storage. 
If a cluster does not have persistent storage support, this value must be set to zero.
 
**Access Modes**: Pods can have read/write access to the persistent volume. 
By default pods have ReadWriteMany access. 
It can be changed from the configuration files by using the configuration parameter: 

    kubernetes.storage.access.mode

The options are ReadWriteMany, ReadWriteOnce and ReadOnlyMany. 
In the case of ReadWriteOnce, only one pod can write. 
In the case of ReadOnlyMany, all pods can only read. 

## Design Issues
Kubernetes has two constructs related to persistent volumes: 
PersistentVolume and PersistentVolumeClaim.  
PersistentVolumes are created either by administrators statically or by provisioners dynamically. 
Twister2 should work in both cases. We assume that either a storage provisioner is deployed 
in the cluster or a static PersistentVolume is created for each job. 
We create one PersistentVolumeClaim instance for each job. 
All pods in the job attach to the same PersistentVolumeClaim. 

When the job has completed, we delete the PersistentVolumeClaim object. 
The provisioner deletes the  PersistentVolume object automatically. 
When the PersistentVolume object is deleted, depending on 
the storage retain policy of the provisioner, created files may be deleted. 
If the job files in the persistent storage are deleted after the job has completed, 
retain policy of the provisioner needs to be changed from “Delete” to “Retain”. 

The name of the PersistentVolumeClaim object for a job is constructed 
by using the job name as a suffix. 

## PersistentVolume Interface: IPersistentVolume
An interface is added to Twister2 package for letting workers access persistent volumes. 
The interface is:
 
   [edu.iu.dsc.tws.common.worker.IPersistentVolume](../../../../../twister2/common/src/java/edu/iu/dsc/tws/common/worker/IPersistentVolume.java)

The interface has methods for two directories: job directory and worker directory. 
Job directory is shared among all workers in a job. Worker directory is created for each worker. 
In addition, it returns the persistent log file name for this worker.
 
The methods are: 

    String getJobDirPath();
    String getWorkerDirPath();
    boolean jobDirExists();
    boolean workerDirExists();
    File getJobDir();
    File getWorkerDir();
    String getLogFileName();

IPersistentVolume interface is implemented in the class:  
  [edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sPersistentVolume](../../../../../twister2/resource-scheduler/src/java/edu/iu/dsc/tws/rsched/schedulers/k8s/worker/K8sPersistentVolume.java)

An instance of this class is passed to the IWorker classes. 
Therefore, workers will be able to use the provided persistent storage.  

## Limitations and Future Works
Persistent storage is only implemented and tested for NFS. 
Kubernetes supports many other distributed storage systems. 
Persistent storage support for other storage systems need to be added. 
Particularly, the support for Amazon AWS, Google GCE, Azure Disk Storage, etc. can be added. 
