
Starting Nomad
--------------

First we need to start Nomad 

```bash
  ./twister2-nomad agent -dev
```

Submit a job
-------------

In order to submit a job, the following command can be used

```bash
  ./twister2 submit standalone ...
```


Log files
---------

In order to view the logs of the nomad agent use the command

```bash
  ./twister2-nomad logs [the allocation id of the task]
```

Useful commands
---------------

Kill workers

```bash
  kill $(jps | grep StandaloneWorkerStarter | awk '{print $1}')
```

