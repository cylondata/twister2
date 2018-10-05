**Running Jobs**

In order to run twister2 jobs you should have a running Mesos cluster.

**Running HelloWorld Example**

Please first familiarize yourself with [HelloWorld job](https://github.com/DSC-SPIDAL/twister2/blob/master/docs/quickstart.md).
You can submit jobs to Kubernetes cluster by using twister2 executable:

```bash
./bin/twister2
```

When submitting jobs to Mesos clusters, you need to specify the cluster
name as "mesos". You can submit HelloWorld job in examples package
with 8 workers as:

```bash
./bin/twister2 submit mesos jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

If there is a problem with job submission, job submission client will
exit with a corresponding message printed to the screen. Otherwise, job
submission client finishes execution with success.

**Job Logs**

We can see the job output either from Mesos Dashboard website(usually it
is on maser ip:5050) or persistent logs. The workers in HelloWorld job
prints a log message and sleeps 1 minutes before exiting.

You the log files from persistent storage if persistent storage is
enabled. Details can be found here on [installation
document](https://github.com/DSC-SPIDAL/twister2/blob/master/docs/deployment/mesos/twister2-mesos-install.md).

**Terminating a Running Job**

While some jobs automatically complete when they finish execution (ex:
batch jobs). Some other jobs may continually run (ex: streaming jobs).
Some jobs may also stuck or take a long time to finish. If we want to
terminate a running job, we can use twister2 command with the job name:

```bash
./bin/twister2 kill mesos jobname
```

