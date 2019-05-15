# Standalone Docker Image

Standalone docker image contains twister2 source code and prebuilt twister2. Examples can be easily run using this image.

We assume that docker is already installed on your machine.

##  Pulling twister2/standalone Image From twister2 dockerhub

dockerhub page for the image: [https://hub.docker.com/r/twister2/standalone](https://hub.docker.com/r/twister2/standalone)

To download the docker image:

```bash
docker pull twister2/standalone
```

## Building twister2/standalone Image Using Dockerfile

If you want to build docker image by yourself, you can download the [Dockerfile](../../../docker/standalone/Dockerfile) and in the same directory, use the command below:

```bash
docker build -t repositoryname:tag .

# example: 
# docker build -t twister2/standalone:0.2.1 .
```

## Running The Container

This command runs the container and starts an interactive bash session

```bash
docker run -it twister2/standalone bash
```

## Running Jobs

Docker image contains the source code and prebuilt twister2. Prebuilt twister2 is in the `~/twister2-0.2.1`  So, make sure that you are in that directory

```bash
cd ~/twister2-0.2.1
```

In order to run a job you can use the following command on the interactive bash session

```bash
twister2 submit slurm job-type job-file-name job-class-name [job-args]
```

Here is an example command

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

In this mode, the job is killed immediately when you terminate the client using ```Ctrl + C```.

## Building Twister2 Inside The Container

Docker image can be also used to build twister2. The source code inside the container is located at `~/twister2`. To build the twister2 inside the docker container:

```bash
cd ~/twister2
bazel build --config=ubuntu scripts/package:tarpkgs
```

This will build twister2 distribution in the file:

```bash
bazel-bin/scripts/package/twister2-0.2.1.tar.gz
```

If you want to modify the twister2 code and test without installing all the dependencies to your machine, you can mount the directory to the docker container:

```bash
docker run -v /path/in/host:/path/in/container -it twister2/standalone bash
```

