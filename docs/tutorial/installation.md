<p align="left">
    <img width="125" height="125" src="fox.png">
</p>


### Geoffrey C. FOX

# Installation Instructions

This section explains how to install the required software to run Twister2 jobs. We will first give the instructions  for standalone installation on a local machine. Then we will explain another installation  option which uses Docker image. The third option is using a twister2-ready cluster on one of our systems, called Echo. We will explain how to use Echo systems to run your jobs.
We will start with a simple Hello World example and then continue with more complex example.

* [Standalone installation](installation.md#standalone-installation-on-local-machine)
* [Docker image based installation](installation.md#docker-image-based-installation)
* [Using Echo cluster](installation.md#using-echo-cluster)


## Standalone installation on local machine

Twister2 relies on the [Bazel build](https://bazel.build/) system to provide a flexible and fast build. Twister2 has been tested on Bazel 0.8.1 version and it is recommended to use it for building.

Twister2 developers are mainly working on Ubuntu 16.04 and Ubuntu 18.04. So it is recommended to use those platforms with the early versions and we would like to expand our system to different platforms in the future.

### Prerequisites

Twister2 build needs several software installed on your system.

1. Operating System
   * Twister2 is tested and known to work on,
     * Red Hat Enterprise Linux Server release 7
     * Ubuntu 14.05, Ubuntu 16.10 and Ubuntu 18.10
2. Java
   * Download Oracle JDK 8 from [http://www.oracle.com/technetwork/java/javase/downloads/index.html](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
   * Extract the archive to a folder named `jdk1.8.0`
   * Set the following environment variables.

     ```text
     JAVA_HOME=<path-to-jdk1.8.0-directory>
     PATH=$JAVA_HOME/bin:$PATH
     export JAVA_HOME PATH
     ```
3. Install the required tools

```bash
   sudo apt-get install g++ git build-essential automake cmake libtool-bin zip libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python-setuptools -y
```

```text
sudo apt-get install  python-dev python-pip
```

1. Installing maven and configure it as follows :

```text
  wget http://mirrors.ibiblio.org/apache/maven/maven-3/3.5.2/binaries/apache-maven-3.5.2-bin.tar.gz
```

Extract this to a directory called maven configure the environmental variables

```text
  MAVEN_HOME=<path-to-maven-directory>
  PATH=$MAVEN_HOME/bin:$PATH
  export MAVEN_HOME PATH
```

1. Install bazel 0.8.1

   ```bash
   wget https://github.com/bazelbuild/bazel/releases/download/0.8.1/bazel-0.8.1-installer-linux-x86_64.sh
   chmod +x bazel-0.8.1-installer-linux-x86_64.sh
   ./bazel-0.8.1-installer-linux-x86_64.sh --user
   ```

   Make sure to add the bazel bin to PATH

   ```text
   export PATH=$PATH:~/bin
   ```

### Compiling Twister2

Now lets get a clone of the source code.

```bash
git clone https://github.com/DSC-SPIDAL/twister2.git
```

You can compile the Twister2 distribution by using the bazel target as below.

```bash
cd twister2
bazel build --config=ubuntu scripts/package:tarpkgs
```

This will build twister2 distribution in the file

```bash
bazel-bin/scripts/package/twister2-client-0.1.0.tar.gz
```

If you would like to compile the twister2 without building the distribution packages use the command

```bash
bazel build --config=ubuntu twister2/...
```

For compiling a specific target such as communications

```bash
bazel build --config=ubuntu twister2/comms/src/java:comms-java
```

### Twister2 Distribution

After you've build the Twister2 distribution, you can extract it and use it to submit jobs.

```bash
cd bazel-bin/scripts/package/
tar -xvf twister2-0.1.0.tar.gz
```

### Compiling OpenMPI

When you compile Twister2 it will automatically download and compile OpenMPI 3.1.2 with it. If you don't like this version of OpenMPI and wants to use your own version, you can compile OpenMPI using following instructions.

* We recommend using `OpenMPI 3.1.2`
* Download OpenMPI 3.0.0 from [https://download.open-mpi.org/release/open-mpi/v3.1/openmpi-3.1.2.tar.gz](https://download.open-mpi.org/release/open-mpi/v3.1/openmpi-3.1.2.tar.gz)
* Extract the archive to a folder named `openmpi-3.1.2`
* Also create a directory named `build` in some location. We will use this to install OpenMPI
* Set the following environment variables

  ```text
  BUILD=<path-to-build-directory>
  OMPI_312=<path-to-openmpi-3.1.2-directory>
  PATH=$BUILD/bin:$PATH
  LD_LIBRARY_PATH=$BUILD/lib:$LD_LIBRARY_PATH
  export BUILD OMPI_312 PATH LD_LIBRARY_PATH
  ```

* The instructions to build OpenMPI depend on the platform. Therefore, we highly recommend looking into the `$OMPI_1101/INSTALL` file. Platform specific build files are available in `$OMPI_1101/contrib/platform` directory.
* In general, please specify `--prefix=$BUILD` and `--enable-mpi-java` as arguments to `configure` script. If Infiniband is available \(highly recommended\) specify `--with-verbs=<path-to-verbs-installation>`. Usually, the path to verbs installation is `/usr`. In summary, the following commands will build OpenMPI for a Linux system.

  ```text
  cd $OMPI_312
  ./configure --prefix=$BUILD --enable-mpi-java
  make -j 8;make install
  ```

* If everything goes well `mpirun --version` will show `mpirun (Open MPI) 3.1.2`. Execute the following command to instal `$OMPI_312/ompi/mpi/java/java/mpi.jar` as a Maven artifact.

  ```text
  mvn install:install-file -DcreateChecksum=true -Dpackaging=jar -Dfile=$OMPI_312/ompi/mpi/java/java/mpi.jar -DgroupId=ompi -DartifactId=ompijavabinding -Dversion=3.1.2
  ```

### FAQ

1. Build fails with ompi java binding errors

Try to do one of these things

Note: If you get an error while compiling or building Twister2 saying "Java bindings requested but no Java support found" please execute the following command to install Java using apt-get command

```bash
  sudo apt install openjdk-8-jdk
```

Or change line 385 of

```bash
third_party/ompi3/ompi.BUILD
```

From

```bash
'./configure --prefix=$$INSTALL_DIR --enable-mpi-java'
```

to

```bash
'./configure --prefix=$$INSTALL_DIR --enable-mpi-java --with-jdk-bindir=<path-to-jdk>/bin --with-jdk-headers=<path-to-jdk>/include',
```

Please replace 'path-to-jdk' with your jdk location.

## Kubernetes Installation

First, you can install Kubernetes project to a machine on your cluster or your personal machine. You need to have kubectl running on the machine you installed the project.

Here are the things that you need to do to run Twister2 jobs on Kubernetes clusters:
* **Authorization of Pods**: This is a required step to run any Twister2 job on Kubernetes clusters.
* **Persistent Storage Settings**: This is optional. You can set up persistent storage only if you want to use.
* **Generating Secret Object for OpenMPI Jobs**: This is required if you are going to run OpenMPI jobs.
* **Providing Rack and Datacenter information**: This is required only if you want Twister2 to perform rack and data center aware scheduling.
* **Job Package Uploader Settings**: This is required only if you want to upload the job package through a web server.

**Requirement**: Twister2 requires Kubernetes version v1.10 or higher.

### Authorization of Pods

Twister2 Worker pods need to watch Job Master pod and get its IP address. In addition, Job Master needs to be able to delete used resources after the job has completed. Therefore, before submitting a job, a Role and a RoleBinding object need to be created. We prepared the following YAML file: twister2-auth.yaml.

If you are going to use the default namespace, then execute the following command:

```bash
    $kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/docs/deployment/kubernetes/twister2-auth.yaml
```

If you are not going to use the default namespace, download the above yaml file, change the namespace field value to a namespace value that your users will use to submit Twister2 jobs. Then, execute the following command:

```bash
    $kubectl create -f /path/to/file/twister2-auth.yaml
```

### Persistent Storage Settings

To enable persistent storage in Twister2, either a Persistent Storage Provisioner or statically configured PersistentVolume must exist in the cluster. Persistent storage class needs to be specified in the client.yaml configuration file. Configuration parameter is:

```bash
    kubernetes.persistent.storage.class
```

We used the default storage class value as "twister2-nfs-storage". Please set your persistent storage class name in your provisioner and in the client.yaml config file.

We tested with NFS-Client provisioner from: [https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client](https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client)

NFS-Client provisioner is used if you already have an NFS server. Otherwise you may use the NFS provisioner that does not require to have an NFS server: [https://github.com/kubernetes-incubator/external-storage/tree/master/nfs](https://github.com/kubernetes-incubator/external-storage/tree/master/nfs)

### Generating Secret Object for OpenMPI Jobs

When using OpenMPI communications in Twister2, pods need to have password-free SSH access among them. This is accomplished by first generating an SSH key pair and deploying them as a Kubernetes Secret on the cluster.

First, generate an SSH key pair by using:

```bash
    $ssh-keygen
```

Second, create a Kubernetes Secret object for the namespace of Twister2 users with the already generated key pairs. Execute the following command by specifying generated key files. Last parameter is the namespace. If you are using a namespace other than default, please change that.

```bash
    $kubectl create secret generic twister2-openmpi-ssh-key --from-file=id_rsa=/path/to/.ssh/id_rsa --from-file=id_rsa.pub=/path/to/.ssh/id_rsa.pub --from-file=authorized_keys=/path/to/.ssh/id_rsa.pub --namespace=default
```

The fifth parameter \(twister2-openmpi-ssh-key\) is the name of the Secret object to be generated. That has to match the following configuration parameter in the network.yaml file:

```bash
    kubernetes.secret.name
```

You can retrieve the created Secret object in YAML form by executing the following command:

```bash
    $kubectl get secret <secret-name> -o=yaml
```

Another possibility for deploying the Secret object is to use the [YAML file template](https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/docs/architecture/resource-schedulers/kubernetes/yaml-templates/secret.yaml). You can edit that secret.yaml file. You can put the public and private keys to the corresponding fields. You can set the name and the namespace values. Then, you can create the Secret object by using kubectl method as:

```bash
    $kubectl create secret -f /path/to/file/secret.yaml
```

### Providing Rack and Datacenter information to Twister2

Twister2 can use rack names and data center names of the nodes when scheduling tasks. There are two ways administrators and users can provide this information.

**Through Configuration Files**:
Users can provide the IP addresses of nodes in racks in their clusters. In addition, they can provide the list of data centers with rack data in them.

Here is an example configuration:

```bash
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

```bash
    kubernetes.node.locations.from.config
```

**Labelling Nodes With Rack and Data Center Information**:
Administrators can label their nodes in the cluster for their rack and datacenter information. Each node in the cluster must be labelled once. When users submit a Twister2 job, submitting client first queries Kubernetes master for the labels of nodes. It provides this list to all workers in the job.

**Note**: For this solution to work, job submitting users must have admin privileges.

**Example Labelling Commands**: Administrators can use kubectl command to label the nodes in the cluster. The format of the label creation command is as follows:

```bash
    >kubectl label node <node-name> <label-key>=<label-value>
```

Then, used rack and data center labels must be provided in the configuration files. These configuration parameters are:

```bash
    rack.labey.key
    datacenter.labey.key
```

To get the rack and datacenter information from Kubernetes master using labels, the value of the following configuration parameter has to be specified as false in client.yaml file:

```bash
    kubernetes.node.locations.from.config
```

### Job Package Uploader Settings

When users submit a Twister2 job in Kubernetes cluster, submitting client program need to transfer the job package to workers. Submitting client first packs all job related files into a tar file. This archive files needs to be transferred to each worker that will be started.

We provide [two methods](../../architecture/resource-schedulers/kubernetes/twister2-on-kubernetes.md):

* Job Package Transfer Using kubectl file copy
* Job Package Transfer Through a Web Server

By default, we use the first method to transfer the job package. This method does not require any installations. It transfers the job package from client to workers directly by using kubectl copy method.
Just make sure that the value of following configuration parameter in uploader.yaml file is true:

```bash
   twister2.kubernetes.client.to.pods.uploading
```

Second method transfers the job package once to a web server running in the cluster. Workers download the job package from this web server.
This method is more efficient since it transfers the job package only once from client machine to a web server.
If the submitting clients are running on the cluster machines, this may not be important.
However, if the submitting clients are running on machines outside the cluster with limited bandwidth, then this can be important.

First, the client to pods uploading parameter has to be disabled by assigning false in uploader.yaml file:

```bash
   twister2.kubernetes.client.to.pods.uploading
```

For the transfer through a web server to work, a web server must exist in the cluster and submitting client must have write permission to that directory.
Then, you need to specify the web server directory and address information for the following configuration parameters in uploader.yaml file:

```bash
   twister2.uploader.scp.command.connection: user@host
   twister2.uploader.directory: "/path/to/web-server/directory/"
   twister2.download.directory: "http://host:port/web-server-directory"
```


## Docker Image Based Installation

will be ready soon ....


## Using Echo Cluster

This option will utilize the already running Twister2 systems including the resource schedulers; Kubernetes and Mesos.
Please follow the instructions below to run jobs on Echo cluster.

Use ssh to login to following Echo machine by using the username and password given to you.

username@149.165.150.84

Twister2 is installed under twister2 directory.

### Running Hello World Example Using Kubernetes

Go to the directory:

*twister2/bazel-bin/scripts/package/twister2-0.1.0/*

In that directory, you will see a script file named;

*submit-k8s-job.sh*

This script runs the following command;

*./bin/twister2 submit kubernetes jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld*

Running that file will submit the HelloWorld job to Kubernetes. You can see the job at the dashboard:

http://149.165.150.81:8080/#/jobs

HelloWorld job starts 4 workers and completes after 1 minutes. After 1 minute, the job becomes COMPLETED if everything is fine.












