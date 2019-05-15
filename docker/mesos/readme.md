## Building Docker Image for Mesos

Build Docker image by running the following command.

```bash
$ docker build -t username/twister2-mesos:docker-mpi .
```

Push generated Docker image to Docker Hub
```bash
$ docker push username/twister2-mesos:docker-mpi
```

This docker image should be pulled on every node that Mesos runs.
```bash
$ docker pull username/twister2-mesos:docker-mpi
```

You can use a web server on a node where you submit twister2 jobs to get
twister2 core and job files into this docker container when you initialize it.

```bash
$ cd twister2
$ wget web_server_ip_address:port_number/twister2/mesos/twister2-core-0.2.1.tar.gz
$ wget web_server_ip_address:port_number/twister2/mesos/twister2-job.tar.gz
```

The other method of getting this files is including them in your docker image.
Unfortunately, this method requires you to create a new docker image everytime
you change twister2 code.

