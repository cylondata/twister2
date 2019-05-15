# Running Jobs

Nomad implementation is experimental at the moment. With this implementation, twister2 can use the 
[Nomad](https://www.nomadproject.io/) scheduler to manage a job. 

## Starting Nomad

First we need to start Nomad. We are shipping nomad binary with Twister2 to make it easy to run Nomad.
That binary is called `twister2-nomad` and can be found in bin directory. Lets start a development version 
of Nomad. 

```bash
  ./bin/twister2-nomad agent -dev
```

## Submit a job

In order to submit a job, the following command can be used

```bash
  ./bin/twister2 submit nomad job-type job-file-name job-class-name [job-args]
```

For example here is a command to run HelloWorld example.

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

## Log files

In order to view the logs of the nomad agent use the command.

```bash
  ./bin/twister2-nomad logs [the allocation id of the task]
```

## Useful commands

If something bad happens and we cannot Kill workers, we can use the command below.

```bash
  kill $(ps ax | grep NomadWorkerStarter | awk '{print $1}')
```

## Nomad UI

You can use Nomad UI to see the jobs running. Run

```bash
./bin/twister2-nomad ui
```

