# Installing Twister2 on Kubernetes

First, you can install Kubernetes project to a machine on your cluster or your personal machine. You need to have kubectl running on the machine you installed the project.

To compile & install Twister2 on a machine, please follow the steps in [compiling document](../../compiling/compiling.md). You may also check [developer document](../../developers/developer-environment.md) for setting up IDEs.

Here are the things that you need to do to run Twister2 jobs on Kubernetes clusters:
* **Authorization of Pods**: This is a required step to run any Twister2 job on Kubernetes clusters. 
* **Deploying Twister2 Dashboard**: This is a recommended step to monitor Twister2 jobs on Kubernetes clusters. 
* **Persistent Storage Settings**: This is optional. You can set up persistent storage only if you want to use. 
* **Generating Secret Object for OpenMPI Jobs**: This is required if you are going to run OpenMPI jobs. 
* **Providing Rack and Datacenter information**: This is required only if you want Twister2 to perform rack and data center aware scheduling. 
* **Job Package Uploader Settings**: This is required only if you want to upload the job package through a web server. 

**Requirement**: Twister2 requires Kubernetes version v1.10 or higher.  

## Authorization of Pods

Twister2 Worker pods need to watch Job Master pod and get its IP address. 
In addition, Job Master needs to be able to delete used resources after the job has completed. 
Therefore, before submitting a job, a Role and a RoleBinding object need to be created. 
We prepared the following YAML file: twister2-auth.yaml.

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
Although it is not mandatory, it is highly recommended. 
A single instance of the Dashboard runs in each cluster. 

There are two versions of Dashboard. One version saves Dashboard state in a persistent storage. 
Another version does not use persistent storage. It looses its content if its pod restarts. 

If you are using default namespace, then you can deploy Dashboard without persistent storage as:

```bash
    $ kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-dashboard-wo-ps.yaml
```

You can deploy Dashboard with persistent storage as:

```bash
    $ kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-dashboard-with-ps.yaml
```

If you are using another namespace, or would like to change a parameter of Dashboard, 
then you can download one of these files, 
change the desired parameter value in them and execute the create command on modified file. 

### Accessing Dashboard

You can access the Dashboard from any machine that has kubectl installed and 
has the credentials to connect to the kubernetes cluster. 

First run the following command in your workstation to create a secure channel: 

```bash
    $ kubectl proxy
```

Then, access Dashboard at the following URL:

```text
    http://localhost:8001/api/v1/namespaces/default/services/http:twister2-dashboard:/proxy/#/
```

If you are using a namespace other than the default, change namespace value in the URL. 

### Running Dashboard as a standalone web server

You can also run Dashboard as a standalone webserver in your cluster. 
All pods should be able to access the machine that is running Dashboard to be able to feed their status data. 

Run Dashboard server with the following command: 

```bash
    $ bin/twister2 dash
```

Dashboard runs at the port 8080. 

You need to give the address of Dashboard at the configuration file: conf/kubernetes/system.yaml 
You should set the value of following parameter:  

```text
    twister2.dashboard.host: "http://<host-address>:8080"
```

## Persistent Storage Settings

To enable persistent storage in Twister2, either a Persistent Storage Provisioner or statically configured PersistentVolume must exist in the cluster. Persistent storage class needs to be specified in the client.yaml configuration file. Configuration parameter is:

```text
    kubernetes.persistent.storage.class
```

We used the default storage class value as "twister2-nfs-storage". Please set your persistent storage class name in your provisioner and in the client.yaml config file.

We tested with NFS-Client provisioner from: [https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client](https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client)

NFS-Client provisioner is used if you already have an NFS server. Otherwise you may use the NFS provisioner that does not require to have an NFS server: [https://github.com/kubernetes-incubator/external-storage/tree/master/nfs](https://github.com/kubernetes-incubator/external-storage/tree/master/nfs)

## Generating Secret Object for OpenMPI Jobs

When using OpenMPI communications in Twister2, pods need to have password-free SSH access among them. This is accomplished by first generating an SSH key pair and deploying them as a Kubernetes Secret on the cluster.

First, generate an SSH key pair by using:

```bash
    $ ssh-keygen
```

Second, create a Kubernetes Secret object for the namespace of Twister2 users with the already generated key pairs. Execute the following command by specifying generated key files. Last parameter is the namespace. If you are using a namespace other than default, please change that. 

```bash
    $ kubectl create secret generic twister2-openmpi-ssh-key --from-file=id_rsa=/path/to/.ssh/id_rsa --from-file=id_rsa.pub=/path/to/.ssh/id_rsa.pub --from-file=authorized_keys=/path/to/.ssh/id_rsa.pub --namespace=default
```

The fifth parameter \(twister2-openmpi-ssh-key\) is the name of the Secret object to be generated. That has to match the following configuration parameter in the network.yaml file:

```text
    kubernetes.secret.name
```

You can retrieve the created Secret object in YAML form by executing the following command:

```bash
    $ kubectl get secret <secret-name> -o=yaml
```

Another possibility for deploying the Secret object is to use the [YAML file template](https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/docs/architecture/resource-schedulers/kubernetes/yaml-templates/secret.yaml). You can edit that secret.yaml file. You can put the public and private keys to the corresponding fields. You can set the name and the namespace values. Then, you can create the Secret object by using kubectl method as:

```bash
    $ kubectl create secret -f /path/to/file/secret.yaml
```

## Providing Rack and Datacenter information to Twister2

Twister2 can use rack names and data center names of the nodes when scheduling tasks. There are two ways administrators and users can provide this information.

**Through Configuration Files**:  
Users can provide the IP addresses of nodes in racks in their clusters. In addition, they can provide the list of data centers with rack data in them.

Here is an example configuration:

```text
    kubernetes.datacenters.list:
    - dc1: ['blue-rack', 'green-rack']
    - dc2: ['rack01', 'rack02']

    kubernetes.racks.list:
    - blue-rack: ['node01.ip', 'node02.ip', 'node03.ip']
    - green-rack: ['node11.ip', 'node12.ip', 'node13.ip']
    - rack01: ['node51.ip', 'node52.ip', 'node53.ip']
    - rack02: ['node61.ip', 'node62.ip', 'node63.ip']
```

Put these lists to client.yaml file. Then, assign the following configuration parameter in client.yaml as true: 

```text
    kubernetes.node.locations.from.config
```

**Labelling Nodes With Rack and Data Center Information**:  
Administrators can label their nodes in the cluster for their rack and datacenter information. Each node in the cluster must be labelled once. When users submit a Twister2 job, submitting client first queries Kubernetes master for the labels of nodes. It provides this list to all workers in the job.

**Note**: For this solution to work, job submitting users must have admin privileges.

**Example Labelling Commands**: Administrators can use kubectl command to label the nodes in the cluster. The format of the label creation command is as follows:

```text
    $ kubectl label node <node-name> <label-key>=<label-value>
```

Then, used rack and data center labels must be provided in the configuration files. These configuration parameters are:

```text
    rack.labey.key
    datacenter.labey.key
```

To get the rack and datacenter information from Kubernetes master using labels, the value of the following configuration parameter has to be specified as false in client.yaml file: 

```text
    kubernetes.node.locations.from.config
```

## Job Package Uploader Settings

When users submit a Twister2 job in Kubernetes cluster, submitting client program need to transfer the job package to workers. Submitting client first packs all job related files into a tar file. This archive files needs to be transferred to each worker that will be started.

We provide [two methods](../../architecture/resource-schedulers/kubernetes/twister2-on-kubernetes.md):

* Job Package Transfer Using kubectl file copy
* Job Package Transfer Through a Web Server

By default, we use the first method to transfer the job package. This method does not require any installations. It transfers the job package from client to workers directly by using kubectl copy method.
Just make sure that the value of following configuration parameter in uploader.yaml file is true: 

```text
   twister2.kubernetes.client.to.pods.uploading
```

Second method transfers the job package once to a web server running in the cluster. Workers download the job package from this web server. 
This method is more efficient since it transfers the job package only once from client machine to a web server. 
If the submitting clients are running on the cluster machines, this may not be important. 
However, if the submitting clients are running on machines outside the cluster with limited bandwidth, then this can be important.

First, the client to pods uploading parameter has to be disabled by assigning false in uploader.yaml file:

```text
   twister2.kubernetes.client.to.pods.uploading
```

For the transfer through a web server to work, a web server must exist in the cluster and submitting client must have write permission to that directory. 
Then, you need to specify the web server directory and address information for the following configuration parameters in uploader.yaml file: 

```text
   twister2.uploader.scp.command.connection: user@host
   twister2.uploader.directory: "/path/to/web-server/directory/"
   twister2.download.directory: "http://host:port/web-server-directory"
```
