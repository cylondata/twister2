**Open MPI Support**

In order to support OpenMPI jobs on twister2, Mesos needs to use
containerizers. The regular way of running jobs will not work with
OpenMPI jobs.  We thought the most feasible solution would be using
docker containers since kubernetes is also using it. Mesos needs the
following features in order to run OpenMPI jobs;

-   Necessary libraries installed on Dockers

-   Docker swarm installation for overlay network among containers

-   Password-free SSH access enabled among Dockers

-   Hostfile generation on MPI master before starting the job

-   MPI command run on MPI master

**Extra Libraries in Pods**

OpenMPI and OpenSSh libraries are installed in the Docker containers
that we use.

**Open mpi install**

    RUN wget https://www.open-mpi.org/software/ompi/v3.0/downloads/openmpi-3.0.0.tar.gz

    RUN tar -zxvf openmpi-3.0.0.tar.gz -C 

    ENV OMPI\_BUILD="/openmpi-build"

    ENV OMPI\_300="/openmpi-3.0.0"

    ENV PATH="${OMPI\_BUILD}/bin:${PATH}"

    ENV LD\_LIBRARY\_PATH="${OMPI\_BUILD}/lib:${LD\_LIBRARY\_PATH}"

    RUN export OMPI\_BUILD OMPI\_300 PATH LD\_LIBRARY\_PATH

    RUN cd /openmpi-3.0.0 && ls -la && ./configure --prefix=$OMPI\_BUILD
    --enable-mpi-java --enable-mpirun-prefix-by-default
    --enable-orterun-prefix-by-default && make;make install

    RUN rm openmpi-3.0.0.tar.gz

**OpenSSH install **

    # Set root password Change it 
    RUN echo 'root:screencast' | chpasswd
    RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

    # SSH login fix. Otherwise user is kicked off after login
    RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

    # install ssh and basic dependencies
    RUN apt-get update && \
        apt-get install -yq --no-install-recommends \
        locales wget ca-certificates ssh build-essential && \
        apt-get install -y g++ && \
        apt-get install -y maven && \
        apt-get clean && apt-get purge && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
        rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

**Password-free SSH Among Pods**

In order to run OpenMPI jobs on Docker containers, password-free SSH
communication should be enabled.

**Hostfile Generation**

OpenMPI needs the host IP addresses, which should be listed in a special
file called the hostfile. Therefore we need to create the host file
before running the OpenMPI run command. This file is created in the
class called MesosMPIMasterStarter.

-   *edu.iu.dsc.tws.rsched.schedulers.Mesos.mpi.MesosMPIMasterStarter*

*Getting the IP addresses of Workers*

Workers are register with the Zookeeper as we have explained it before.
When the workers register with the zookeeper, it will know the IP
addresses and the port numbers of the workers. For this reason, we use
Zookeeper to get the necessary information to create the host file on
MPI master.

**Starting MPI Job**

MPI command is generated on the worker which runs as a MPI master. This
command is;

    String[] command = {"mpirun", "-allow-run-as-root", "-npernode",
        "1", "--mca", "btl_tcp_if_include", "eth0",
        "--hostfile", "/twister2/hostFile", "java", "-cp",
        "twister2-job/libexamples-java.jar:twister2-core/lib/*",
        mpiClassNameToRun, mpiMaster.jobName, jobMasterIP};

Meaning of some of the parameters;

-npernode "x": defines the number of mpi workers run on each mesos
workers.

"--mca", "btl\_tcp\_if\_include", "eth0" : states mpi to use
specific network. We need this since Mesos needs an overlay network to
run OpenMPI jobs as explained before.

mpiClassNameToRun: This parameter is used to state the name of the class
to run in MPI. In our case this class is "MesosMPIWorkerStarter". We
start the MPI job in this class as explained below.

**MesosMPIWorkerStarter Class**

This class is used to run an MPI job on each MPI worker. MPI
initialization and getting the ranking for the workers are done here.

    MPI.Init(args);
    workerID = MPI.COMM_WORLD.getRank();

After the initialization, logging and jobmaster client are initialized.
The last thing is the starting MPI job by creating the resource plan and
calling the init method of the MPI job class.

**Configuration Parameters**

The following configuration parameters are needed to be set in order to
run open mpi jobs.

The parameters are:

In resource.yaml;

    #overlay network name for docker containers
    twister2.mesos.overlay.network.name: "mesos-overlay"

    #Docker image name
    twister2.docker.image.name: "twister2/twister2-mesos:docker-mpi"

In client.yaml;

    #Mesos will use docker container if the value is true.
    twister2.use_docker_container: "true"

