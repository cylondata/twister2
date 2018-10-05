# Running Jobs

We assume that you have a running Kubernetes cluster. In your machine, kubectl is configured to talk to Kubernetes master.

## Running HelloWorld Example

Please first familiarize yourself with [HelloWorld job](../../quickstart.md). You can submit jobs to Kubernetes cluster by using twister2 executable:

```text
./bin/twister2
```

When submitting jobs to Kubernetes clusters, you need to specify the cluster name as "kubernetes". You can submit HelloWorld job in examples package with 8 workers as:

```text
./bin/twister2 submit kubernetes jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

If there is a problem with job submission, job submission client will exit with a corresponding message printed to the screen. Otherwise, job submission client finishes execution with success.

## Job Logs

We can see the job output either from Kubernetes Dashboard website or persistent logs. The workers in HelloWorld job prints a log message and sleeps 1 minutes before exiting. So the user can check Kubernetes Dashboard website for worker log messages. There must be a StatefulSet with the job name. List the pods in that StatefulSet. Check the output for each pod by clicking on the right hand side button. You will see the output for each worker over there.

You can also check the log files from persistent storage if persistent storage is enabled. As it is explained in the [installation document](twister2-kubernetes-install.md), you need to have a persistent storage provisioner in your cluster. Then, you need to specify the storage class in your configuration file:

```text
kubernetes.persistent.storage.class
```

Then, you need to learn the persistent logging directory of your storage provisioner. You can learn it from Kubernetes Dashboard by checking the provisioner entity or consulting your administrator. Check that directory for the job logs.

## Logs for MPI Enabled Jobs

The outputs of MPI enabled jobs are a little different in Kubernetes Dashboard. In those jobs, first worker starts all other workers by using mpirun command. Therefore, the outputs of all other workers are transferred to the first worker to show on the screen of Kubernetes Dashboard. So, all outputs from all workers in an MPI enabled job is shown in first worker dashboard. The dashboards of other workers will only have initialization messages.

However, persistent log files are different. In those files, each worker has its own log messages in its log file. Therefore, checking log files would be more preferable in MPI enabled jobs compared to checking the dashboard.

## Terminating a Running Job

While some jobs automatically complete when they finish execution \(ex: batch jobs\). Some other jobs may continually run \(ex: streaming jobs\). Some jobs may also stuck or take a long time to finish. If we want to terminate a running job, we can use twister2 command with the job name:

```text
./bin/twister2 kill kubernetes job-1
```

This command kills the job with the name: "job-1"

## Configuration Settings

Configuration files for kubernetes clusters are under the directory:

```text
conf/kubernetes/
```

You can specify job related configurations either through client.yaml file or in your job java file. For example, job name can be specified in both those locations. Java file has precedence over the conf files if you specify in both locations.

## Job Names

Since we are using job names as also StatefulSet names in Kubernetes, job names must follow [Kubernetes resource naming rules](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/).

Job names should consist of lower case alphanumeric characters, dash\(-\), and dot\(.\). Otherwise job submission will fail.

