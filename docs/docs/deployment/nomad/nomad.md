---
id: nomad
title: Nomad
sidebar_label: Nomad
---

# Installing Twister2 on Nomad

Nomad is a flexible container management tool that enables easy deployment of containerized or legacy applications.  As in other resource schedulers, Nomad abstracts away machines and locations, enabling users to declare what to run while Nomad handles where and how to run them. 

In order to run twister2 jobs using Nomad, you need to have Nomad  installed on your cluster. 


* **Persistent Storage Settings**: This is optional. You can set up persistent storage only if you want to use. 
* **Providing Rack and Datacenter information**: This is required only if you want Twister2 to perform rack and data center aware scheduling. 
* **Job Package Uploader Settings**: This is required only if you want to upload the job package through a web server. 
* **Required Configuration Parameters**: There are some parameters that should be set.
* **Deploying Twister2 Dashboard**: This option is not required but a recommended step to monitor Twister2 jobs on Nomad clusters. 

## Deploying Twister2 Dashboard

Twister2 Dashboard enables users to monitor their jobs through a web browser. 
It is not mandatory. A single instance of the Dashboard runs in each cluster. 



## Persistent Storage Settings

Twister2 expects that either a Persistent Storage Provisioner or statically configured PersistentVolume exists in the cluster. Our Nomad system uses NFS for persistent storage to store logs and outputs of twister2 jobs.
 
 You have to set the following parameters in client.yaml config file in order to use NFS.

```text
    # nfs server address
    nfs.server.address: "149.165.150.81"
    
    # nfs server path
    nfs.server.path: "/nfs/shared/twister2"

```

## Providing Rack and Datacenter information to Twister2

Twister2 can use rack names and data center names of the nodes when scheduling tasks. There are two ways administrators and users can provide this information.

**Through Configuration Files**:  
Users can provide the IP addresses of nodes in racks in their clusters. In addition, they can provide the list of data centers with rack data in them.

Here is an example configuration:

```text
    datacenters.list:
    - dc1: ['blue-rack', 'green-rack']
    - dc2: ['rack01', 'rack02']

    racks.list:
    - blue-rack: ['node01.ip', 'node02.ip', 'node03.ip']
    - green-rack: ['node11.ip', 'node12.ip', 'node13.ip']
    - rack01: ['node51.ip', 'node52.ip', 'node53.ip']
    - rack02: ['node61.ip', 'node62.ip', 'node63.ip']
```

## Job Package Uploader Settings

When users submit a job to Nomad master, they first need to transfer the job package to workers. Submitting client packs all job related files into a tar file. This archive files needs to be transferred to each worker that will be started. This transfer is done through a web server in Nomad.
To do this there should be a web server running in the cluster. In addition, job submitting client must have write permission to that directory. Workers download the job package from this web server. You need to specify the web server directory and address information the uploader.yaml file.

```text
 # the directory where the file will be uploaded, make sure the user has the necessary permissions
 # to upload the file here.
 twister2.uploader.directory: "/var/www/html/twister2/nomad/"
 
 # This is the scp command options that will be used by the uploader, this can be used to
 # specify custom options such as the location of ssh keys.
 twister2.uploader.scp.command.options: "--chmod=+rwx"
 
 # The scp connection string sets the remote user name and host used by the uploader.
 twister2.uploader.scp.command.connection: "root@149.165.150.81"
 
 # The ssh command options that will be used when connecting to the uploading host to execute
 # command such as delete files, make directories.
 twister2.uploader.ssh.command.options: ""
 
 # The ssh connection string sets the remote user name and host used by the uploader.
 twister2.uploader.ssh.command.connection: "root@149.165.150.81"
 
 # this is the method that workers use to download the core and job packages
 # it could be  LOCAL,  HTTP, HDFS, ..
 twister2.uploader.download.method: "HTTP"
 
 # The following two parameters are required for system to locate and use the core and job packages prepared for twister2 if HTTP method is used. 
 
 twister2.job.package.url: "http://149.165.150.81:8082/twister2/nomad/twister2-job.tar.gz"
  twister2.core.package.url: "http://149.165.150.81:8082/twister2/nomad/twister2-core-0.2.2.tar.gz"
 

```

## Required Configuration Parameters

The following parameters is used to set the address of the Nomad master. If you have it installed on your local machine then use localhost:4646 as an address. 

```text
# The URI of Nomad master
twister2.nomad.scheduler.uri: "http://149.165.150.81:4646"
```

This parameter is used to locate the core package of twister2. This location should have read and write permissions for the user running twister2 jos.

```text 
# path to the system core package
twister2.system.package.uri: "${TWISTER2_DIST}/twister2-core-0.2.2.tar.gz"
```


# Running Jobs


## Submit a job

In order to submit a job, the following command can be used

```bash
  ./bin/twister2 submit nomad job-type job-file-name job-class-name [job-args]
```

For example here is a command to run HelloWorld example.

```bash
./bin/twister2 submit nomad jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

## Log files

In order to view the logs of the nomad agent use the command.

```bash
  ./bin/twister2-nomad logs [the allocation id of the task]
```

## Nomad UI

You can go to the web ui of your currently installed nomad scheduler. Which is at http://ip_address:4646

## Job Logs

You can see the job output either from Nomad Dashboard website or persistent logs if enabled. 
The workers in HelloWorld job prints a log message and sleeps 1 minutes before exiting. 
So the user can check Nomad Dashboard website(http://ip_address:4646) for worker log messages. 

You can also check the log files from persistent storage if persistent storage is enabled.
You need to learn the persistent logging directory of your storage provisioner in order to do that. 

## Killing a Twister2 Job

While some jobs automatically complete when they finish execution \(ex: batch jobs\), 
some other jobs may continually run \(ex: streaming jobs\). Some jobs may also stuck or take a long time to finish. 
You can use the following command to kill it. 

```bash
  kill $(ps ax | grep NomadWorkerStarter | awk '{print $1}')
```

## Job Names

We are using job names as labels. Job names should consist of lower case alphanumeric characters and dash\(-\) only. Their length can be 50 chars at most. 
If job names do not conform to these rules, we automatically change them to accommodate those rules. We use the changed names as job names.  
