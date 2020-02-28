---
id: job-submit
title: Job Submit
sidebar_label: Job Submit
---

One of our primary goals is to make running Twister2 jobs as simple as possible. 
If you downloaded a twister2 distribution, you just need to unpack that tar file.

If you compiled the source code, following tar package is generated under twister2 directory:
```text
bazel-bin/scripts/package/twister2-0.4.0.tar.gz
```

Unpack the downloaded or generated tar file: 
```bash
$ tar xf bazel-bin/scripts/package/twister2-0.4.0.tar.gz
```

## Submitting a Job

Twister2 jobs are submitted using the ```twister2``` command. This command is found inside the bin
directory of the distribution.

Here is a description of the command

```bash
twister2 submit cluster-type job-type job-file-name job-class-name [job-args]
```

* submit: the command to execute
* cluster-type: either of standalone, kubernetes, nomad, slurm, mesos
* job-type: at the moment, we only support jar
* job-file-name: the file path of the job file (the jar file)
* job-class-name: name of the job class with a main method to execute

Here is an example command:

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 80 -workers 4 -size 1000 -op "allgather" -stages 8,1
```

## Terminating a job

To kill a job, ```twister2 kill`` command is used.

Here is a description of the command:

```bash
twister2 kill cluster-type <job-id>
```

* kill: the command to execute
* cluster-type: either of standalone, kubernetes, nomad, slurm, mesos
* job-id: id of the job to kill

Note: when jobs complete, they are terminated automatically. 
Kill command is used to kill jobs that does not terminate. 

## Listing jobs

When ZooKeeper is used to store job metadata, this data stays at ZooKeeper server 
even after the job has completed. Users can query ZooKeeper server and see the status 
of current and past jobs. 

Here is a description of the command:

```bash
twister2 list <jobs/job-id>
```

* list: the command to execute
* jobs/job-id: if the word "jobs" is given as the third parameter, current and past jobs are listed. 
If the job-id is given as the third parameter, then the status of that job is listed with its workers. 

For this command to work, ZooKeeper address has to be provided as the value of following parameter 
in the common/resource.yaml file:
```text
twister2.resource.zookeeper.server.addresses
```

### Listing Jobs in Kubernetes Clusters

When running jobs in Kubernetes clusters, sometimes ZooKeeper server may run in Kubernetes cluster
and there may not be a direct connection from user machine to ZooKeeper server. 
In such cases, users can run the list command in the cluster by creating a twister2 pod and 
executing the commands in that pod. 

We have designed a sample twister2 pod for this purpose. You can create that pod as: 

```bash
kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-pod.yaml 
```

After creating the pod, you can set your ZooKeeper address at the config file as:
```bash
kubectl exec -it twister2-pod -- nano conf/common/resource.yaml
```

You can list the jobs as:
```bash
kubectl exec -it twister2-pod -- bin/twister2 list jobs
```

You can list a particular job assuming the job-id as: username-t2-job-gnrk4yt
```bash
kubectl exec -it twister2-pod -- bin/twister2 list username-t2-job-gnrk4yt
```

Do not forget to delete the pod after you are done with it:
```bash
kubectl delete -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-pod.yaml 
```
