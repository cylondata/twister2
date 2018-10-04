# Submitting Jobs

One of our primary goals is to make the installation of Twister2 as simple as possible. We are on our way to achieve that goal but we are not fully there yet.

Once you have downloaded a twister2 distribution or compile it from the source, all you need to do is extract that distribution.

## Submitting a Job

Twister2 jobs are submitted using the ```twister2``` command. This command is found inside the bin
directory of the distribution.

Here is a description of the command

```bash
twister2 submit cluster job-type job-file-name job-class-name [job-args]
```

* submit is the command to execute
* cluster which resource manager to use, i.e. standalone, kubernetes, this should be the name of the configuration directory for that particular resource manager
* job-type at the moment we only support jar
* job-file-name the file path of the job file (the jar file)
* job-class-name name of the job class with a main method to execute

Here is an example command

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 80 -workers 4 -size 1000 -op "allgather" -stages 8,1
```

In this command, cluster is standalone and has program arguments.

## Killing a job

To kill a job, ```twister2 kill`` command is used.

Here is a description of the command

```bash
twister2 kill cluster <job-name>
```

* kill the command to execute
* cluster which resource manager to use, i.e. standalone, kubernetes, this should be the name of the configuration directory for that particular resource manager
* job-name name of the job



