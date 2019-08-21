---
id: configurations
title: Twister2 Configurations
sidebar_label: Configurations
---
## Common configurations
### Common Checkpoint Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.checkpointing.enable</td><td>false</td><td>Enable or disable checkpointing</td><tbody><tr><td>twister2.checkpointing.store</td><td>edu.iu.dsc.tws.checkpointing.stores.LocalFileStateStore</td><td>The implementation of the store to be used</td><tbody><tr><td>twister2.checkpointing.store.fs.dir</td><td>"${TWISTER2_HOME}/persistent/"</td><td>Root directory of local file system based store</td><tbody><tr><td>twister2.checkpointing.store.hdfs.dir</td><td>"/twister2/persistent/"</td><td>Root directory of hdfs based store</td><tbody><tr><td>twister2.checkpointing.source.frequency</td><td>1000</td><td>Source triggering frequency</td></tbody></table>

### Common Data Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.data.hadoop.home</td><td>"${HADOOP_HOME}"</td><td></td><tbody><tr><td>twister2.data.hdfs.url</td><td>"hdfs://namenode:9000"</td><td></td><tbody><tr><td>twister2.data.hdfs.class</td><td>"org.apache.hadoop.hdfs.DistributedFileSystem"</td><td></td><tbody><tr><td>twister2.data.hdfs.implementation.key</td><td>"fs.hdfs.impl"</td><td></td><tbody><tr><td>twister2.data.hdfs.config.directory</td><td>"${HADOOP_HOME}/etc/hadoop/core-site.xml"</td><td></td><tbody><tr><td>twister2.data.hdfs.data.directory</td><td>"/user/username/"</td><td></td><tbody><tr><td>twister2.data.hdfs.namenode</td><td>"namenode.domain.name"</td><td></td><tbody><tr><td>twister2.data.hdfs.namenode.port</td><td>"9000"</td><td></td></tbody></table>

### Common Network Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.network.buffer.size</td><td>1024000</td><td>the buffer size to be used</td><tbody><tr><td>twister2.network.sendBuffer.count</td><td>4</td><td>number of send buffers to be used</td><tbody><tr><td>twister2.network.receiveBuffer.count</td><td>4</td><td>number of receive buffers to be used</td><tbody><tr><td>twister2.network.channel.pending.size</td><td>2048</td><td>channel pending messages</td><tbody><tr><td>twister2.network.send.pending.max</td><td>4</td><td>the send pending messages</td><tbody><tr><td>twister2.network.partition.message.group.low_water_mark</td><td>8000</td><td>group up to 8 ~ 16 messages</td><tbody><tr><td>twister2.network.partition.message.group.high_water_mark</td><td>16000</td><td>this is the max number of messages to group</td><tbody><tr><td>twister2.network.partition.batch.grouping.size</td><td>10000</td><td>in batch partition operations, this value will be used to create mini batches<br/>within partial receivers</td><tbody><tr><td>twister2.network.ops.persistent.dirs</td><td>["${TWISTER2_HOME}/persistent/"]</td><td>For disk based operations, this directory list will be used to persist incoming messages.<br/>This can be used to balance the load between multiple devices, by specifying directory locations<br/>from different devices.</td><tbody><tr><td>twister2.network.shuffle.memory.bytes.max</td><td>102400000</td><td>the maximum amount of bytes kept in memory for operations that goes to disk</td><tbody><tr><td>twister2.network.shuffle.memory.records.max</td><td>102400000</td><td>the maximum number of records kept in memory for operations that goes to dist</td><tbody><tr><td>twister2.network.shuffle.file.bytes.max</td><td>10000000</td><td>size of the shuffle file (10MB default)</td><tbody><tr><td>twister2.network.shuffle.parallel.io</td><td>2</td><td>no of parallel IO operations permitted</td><tbody><tr><td>twister2.network.partition.algorithm.stream</td><td>"simple"</td><td>the partitioning algorithm</td><tbody><tr><td>twister2.network.partition.algorithm.batch</td><td>"simple"</td><td>the partitioning algorithm</td><tbody><tr><td>twister2.network.partition.algorithm.batch.keyed_gather</td><td>"simple"</td><td>the partitioning algorithm</td><tbody><tr><td>ttwister2.network.partition.ring.group.workers</td><td>2</td><td>ring group worker</td></tbody></table>

### Common Resource Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.client.debug</td><td>'-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006'</td><td>use this property to debug the client submitting the job</td></tbody></table>

### Common Core Configurations



#### Twister2 Job Master related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.job.master.used</td><td>true</td><td></td><tbody><tr><td>twister2.job.master.runs.in.client</td><td>true</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster<br/>by default, it is true<br/>when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td><tbody><tr><td>twister2.job.master.assigns.worker.ids</td><td>false</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when regitering with the job master</td><tbody><tr><td>twister2.worker.to.job.master.response.wait.duration</td><td>100000</td><td></td></tbody></table>

#### WorkerController related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.worker.controller.max.wait.time.for.all.workers.to.join</td><td>100000</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td><tbody><tr><td>twister2.worker.controller.max.wait.time.on.barrier</td><td>100000</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></tbody></table>

#### Common thread pool config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.common.thread.pool.threads</td><td>2</td><td>Maximum number of threads to spawn on demand</td><tbody><tr><td>twister2.common.thread.pool.keepalive</td><td>10</td><td>maximum time that excess idle threads will wait for new tasks before terminating</td></tbody></table>

#### Dashboard related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.dashboard.host</td><td>"http://localhost:8080"</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></tbody></table>

### Common Task Configurations



#### Task Scheduler Related Configurations
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.taskscheduler.streaming</td><td>"roundrobin"</td><td>Task scheduling mode for the streaming jobs "roundrobin" or "firstfit" or "datalocalityaware" or "userdefined"<br/>By default it is roundrobin mode.</td><tbody><tr><td>twister2.taskscheduler.streaming.class</td><td>"edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler"</td><td>Task Scheduler class for the round robin streaming task scheduler</td><tbody><tr><td>twister2.taskscheduler.streaming.class</td><td>"edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler"</td><td>Task Scheduler for the Data Locality Aware Streaming Task Scheduler</td><tbody><tr><td>twister2.taskscheduler.streaming.class</td><td>"edu.iu.dsc.tws.tsched.streaming.firstfit.FirstFitStreamingTaskScheduler"</td><td>Task Scheduler for the FirstFit Streaming Task Scheduler</td><tbody><tr><td>twister2.taskscheduler.streaming.class</td><td>"edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler"</td><td>Task Scheduler for the userDefined Streaming Task Scheduler</td><tbody><tr><td>twister2.taskscheduler.batch</td><td>"roundrobin"</td><td>Task scheduling mode for the batch jobs "roundrobin" or "datalocalityaware" or "userdefined"<br/>By default it is roundrobin mode.</td><tbody><tr><td>twister2.taskscheduler.batch.class</td><td>"edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler"</td><td>Task Scheduler class for the round robin batch task scheduler</td><tbody><tr><td>twister2.taskscheduler.batch.class</td><td>"edu.iu.dsc.tws.tsched.batch.datalocalityaware.DataLocalityBatchTaskScheduler"</td><td>Task Scheduler for the Data Locality Aware Batch Task Scheduler</td><tbody><tr><td>twister2.taskscheduler.batch.class</td><td>"edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler"</td><td>Task Scheduler for the userDefined Batch Task Scheduler</td><tbody><tr><td>twister2.taskscheduler.task.instances</td><td>2</td><td>Number of task instances to be allocated to each worker/container</td><tbody><tr><td>twister2.taskscheduler.task.instance.ram</td><td>512.0</td><td>Ram value to be allocated to each task instance</td><tbody><tr><td>twister2.taskscheduler.task.instance.disk</td><td>500.0</td><td>Disk value to be allocated to each task instance</td><tbody><tr><td>twister2.taskscheduler.instance.cpu</td><td>2.0</td><td>CPU value to be allocated to each task instancetwister2.task.parallelism</td><tbody><tr><td>twister2.taskscheduler.container.instance.ram</td><td>4096.0</td><td>Default Container Instance Values<br/>Ram value to be allocated to each container</td><tbody><tr><td>twister2.taskscheduler.container.instance.disk</td><td>8000.0</td><td>Disk value to be allocated to each container</td><tbody><tr><td>twister2.taskscheduler.container.instance.cpu</td><td>16.0</td><td></td><tbody><tr><td>twister2.taskscheduler.ram.padding.container</td><td>2.0</td><td>Default Container Padding Values<br/>Default padding value of the ram to be allocated to each container</td><tbody><tr><td>twister2.taskscheduler.disk.padding.container</td><td>12.0</td><td>Default padding value of the disk to be allocated to each container</td><tbody><tr><td>twister2.taskscheduler.cpu.padding.container</td><td>1.0</td><td>CPU padding value to be allocated to each container</td><tbody><tr><td>twister2.taskscheduler.container.padding.percentage</td><td>2</td><td>Percentage value to be allocated to each container</td><tbody><tr><td>twister2.taskscheduler.container.instance.bandwidth</td><td>100 #Mbps</td><td>Static Default Network parameters<br/>Bandwidth value to be allocated to each container instance for datalocality scheduling</td><tbody><tr><td>twister2.taskscheduler.container.instance.latency</td><td>0.002 #Milliseconds</td><td>Latency value to be allocated to each container instance for datalocality scheduling</td><tbody><tr><td>twister2.taskscheduler.datanode.instance.bandwidth</td><td>200 #Mbps</td><td>Bandwidth to be allocated to each datanode instance for datalocality scheduling</td><tbody><tr><td>twister2.taskscheduler.datanode.instance.latency</td><td>0.01 #Milliseconds</td><td>Latency value to be allocated to each datanode instance for datalocality scheduling</td><tbody><tr><td>twister2.taskscheduler.task.parallelism</td><td>2</td><td>Prallelism value to each task instance</td><tbody><tr><td>twister2.taskscheduler.task.type</td><td>"streaming"</td><td>Task type to each submitted job by default it is "streaming" job.</td><tbody><tr><td>twister2.exector.worker.threads</td><td>1</td><td>number of threads per worker</td><tbody><tr><td>twister2.executor.batch.name</td><td>"edu.iu.dsc.tws.executor.threading.BatchSharingExecutor2"</td><td>name of the batch executor</td><tbody><tr><td>twister2.exector.instance.queue.low.watermark</td><td>10000</td><td>number of tuples executed at a single pass</td></tbody></table>

## Standalone configurations
### Standalone Checkpoint Configurations



No specific configurations
### Standalone Data Configurations



No specific configurations
### Standalone Network Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.network.channel.class</td><td>"edu.iu.dsc.tws.comms.mpi.TWSMPIChannel"</td><td></td></tbody></table>

### Standalone Resource Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.resource.scheduler.mpi.working.directory</td><td>"${HOME}/.twister2/jobs"</td><td>working directory</td><tbody><tr><td>twsiter2.resource.scheduler.mpi.mode</td><td>"standalone"</td><td>mode of the mpi scheduler</td><tbody><tr><td>twister2.resource.scheduler.mpi.job.id</td><td>""</td><td>the job id file</td><tbody><tr><td>twister2.resource.scheduler.mpi.shell.script</td><td>"mpi.sh"</td><td>slurm script to run</td><tbody><tr><td>twister2.resource.scheduler.mpi.home</td><td>""</td><td>the mpirun command location</td><tbody><tr><td>twister2.system.package.uri</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><td>the package uri</td><tbody><tr><td>twister2.class.launcher</td><td>"edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher"</td><td>the launcher class</td><tbody><tr><td>twister2.resource.scheduler.mpi.mpirun.file</td><td>"twister2-core/ompi/bin/mpirun"</td><td>mpi run file, this assumes a mpirun that is shipped with the product<br/>change this to just mpirun if you are using a system wide installation of OpenMPI<br/>or complete path of OpenMPI in case you have something custom</td><tbody><tr><td>twister2.resource.scheduler.mpi.mapby</td><td>"node"</td><td>mpi scheduling policy. Two possible options are node and slot.<br/>read more at https://www.open-mpi.org/faq/?category=running#mpirun-scheduling</td><tbody><tr><td>twister2.resource.scheduler.mpi.mapby.use-pe</td><td>false</td><td>use mpi map-by modifier PE. If this option is enabled, cpu count of compute resource<br/>specified in job definition will be taken into consideration</td><tbody><tr><td>twister2.resource.sharedfs</td><td>true</td><td>Indicates whether bootstrap process needs to be run and distribute job file and core<br/>between MPI nodes. Twister2 assumes job file is accessible to all nodes if this property is set<br/>to true, else it will run the bootstrap process</td><tbody><tr><td>twister2.resource.fs.mount</td><td>"${TWISTER2_HOME}/persistent/fs/"</td><td>Directory for file system volume mount</td><tbody><tr><td>twister2.uploader.directory</td><td>"${HOME}/.twister2/repository"</td><td>the uploader directory</td><tbody><tr><td>twister2.class.uploader</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><td>the uplaoder class</td><tbody><tr><td>twister2.uploader.download.method</td><td>"HTTP"</td><td>this is the method that workers use to download the core and job packages<br/>it could be  HTTP, HDFS, ..</td></tbody></table>

### Standalone Core Configurations



#### Twister2 Job Master related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.job.master.used</td><td>true</td><td></td><tbody><tr><td>twister2.job.master.runs.in.client</td><td>true</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster<br/>by default, it is true<br/>when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td><tbody><tr><td>twister2.job.master.assigns.worker.ids</td><td>false</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when regitering with the job master</td><tbody><tr><td>twister2.worker.to.job.master.response.wait.duration</td><td>100000</td><td></td></tbody></table>

#### WorkerController related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.worker.controller.max.wait.time.for.all.workers.to.join</td><td>100000</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td><tbody><tr><td>twister2.worker.controller.max.wait.time.on.barrier</td><td>100000</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></tbody></table>

#### Common thread pool config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.common.thread.pool.threads</td><td>2</td><td>Maximum number of threads to spawn on demand</td><tbody><tr><td>twister2.common.thread.pool.keepalive</td><td>10</td><td>maximum time that excess idle threads will wait for new tasks before terminating</td></tbody></table>

#### Dashboard related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.dashboard.host</td><td>"http://localhost:8080"</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></tbody></table>

### Standalone Task Configurations



No specific configurations
## Slurm configurations
### Slurm Checkpoint Configurations



No specific configurations
### Slurm Data Configurations



No specific configurations
### Slurm Network Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.network.channel.class</td><td>"edu.iu.dsc.tws.comms.mpi.TWSMPIChannel"</td><td></td></tbody></table>

### Slurm Resource Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.resource.scheduler.mpi.working.directory</td><td>"${HOME}/.twister2/jobs"</td><td>working directory</td><tbody><tr><td>twsiter2.resource.scheduler.mpi.mode</td><td>"slurm"</td><td>mode of the mpi scheduler</td><tbody><tr><td>twister2.resource.scheduler.mpi.job.id</td><td>""</td><td>the job id file</td><tbody><tr><td>twister2.resource.scheduler.mpi.shell.script</td><td>"mpi.sh"</td><td>slurm script to run</td><tbody><tr><td>twister2.resource.scheduler.slurm.partition</td><td>"juliet"</td><td>slurm partition</td><tbody><tr><td>twister2.resource.scheduler.mpi.home</td><td>""</td><td>the mpirun command location</td><tbody><tr><td>twister2.system.package.uri</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><td>the package uri</td><tbody><tr><td>twister2.class.launcher</td><td>"edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher"</td><td>the launcher class</td><tbody><tr><td>twister2.resource.scheduler.mpi.mpirun.file</td><td>"twister2-core/ompi/bin/mpirun"</td><td>mpi run file, this assumes a mpirun that is shipped with the product<br/>change this to just mpirun if you are using a system wide installation of OpenMPI<br/>or complete path of OpenMPI in case you have something custom</td><tbody><tr><td>twister2.uploader.directory</td><td>"${HOME}/.twister2/repository"</td><td>the uploader directory</td><tbody><tr><td>twister2.class.uploader</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><td>the uplaoder class</td></tbody></table>

### Slurm Core Configurations



#### WorkerController related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.worker.controller.max.wait.time.for.all.workers.to.join</td><td>100000</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td><tbody><tr><td>twister2.worker.controller.max.wait.time.on.barrier</td><td>100000</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></tbody></table>

#### Dashboard related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.dashboard.host</td><td>"http://localhost:8080"</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></tbody></table>

### Slurm Task Configurations



No specific configurations
## Aurora configurations
### Aurora Checkpoint Configurations



No specific configurations
### Aurora Data Configurations



No specific configurations
### Aurora Network Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.network.channel.class</td><td>"edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"</td><td></td></tbody></table>

### Aurora Resource Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.system.package.uri</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><td>the package uri</td><tbody><tr><td>twister2.class.launcher</td><td>edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraLauncher</td><td>launcher class for aurora submission</td><tbody><tr><td>twister2.class.uploader</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"</td></tr><tr><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td></tr></tbody></table><td>the uploader class</td><tbody><tr><td>twister2.job.worker.class</td><td>"edu.iu.dsc.tws.examples.internal.rsched.BasicAuroraContainer"</td><td>container class to run in workers</td><tbody><tr><td>twister2.class.aurora.worker</td><td>"edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraWorkerStarter"</td><td>the Aurora worker class</td></tbody></table>

#### ZooKeeper related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.zookeeper.server.addresses</td><td>"149.165.150.81:2181"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td></tr></tbody></table><td></td><tbody><tr><td>#twister2.zookeeper.root.node.path</td><td>"/twister2"</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td><tbody><tr><td>twister2.zookeeper.max.wait.time.for.all.workers.to.join</td><td>100000</td><td>if the workers want to wait for all others to join a job, max wait time in ms</td></tbody></table>

#### Uploader configuration
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.uploader.directory</td><td>"/root/.twister2/repository/"</td><td>the directory where the file will be uploaded, make sure the user has the necessary permissions<br/>to upload the file here.</td><tbody><tr><td>twister2.uploader.directory.repository</td><td>"/root/.twister2/repository/"</td><td></td><tbody><tr><td>twister2.uploader.scp.command.options</td><td>""</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td><tbody><tr><td>twister2.uploader.scp.command.connection</td><td>"root@149.165.150.81"</td><td>The scp connection string sets the remote user name and host used by the uploader.</td><tbody><tr><td>twister2.uploader.ssh.command.options</td><td>""</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td><tbody><tr><td>twister2.uploader.ssh.command.connection</td><td>"root@149.165.150.81"</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td></tbody></table>

#### Client configuration parameters for submission of twister2 jobs
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.resource.scheduler.aurora.script</td><td>"${TWISTER2_CONF}/twister2.aurora"</td><td>aurora python script to submit a job to Aurora Scheduler<br/>its default value is defined as the following in the code<br/>can be reset from this config file if desired</td><tbody><tr><td>twister2.resource.scheduler.aurora.cluster</td><td>"example"</td><td>cluster name aurora scheduler runs in</td><tbody><tr><td>twister2.resource.scheduler.aurora.role</td><td>"www-data"</td><td>role in cluster</td><tbody><tr><td>twister2.resource.scheduler.aurora.env</td><td>"devel"</td><td>environment name</td><tbody><tr><td>twister2.job.name</td><td>"basic-aurora"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"basic-aurora"</td></tr></tbody></table><td>aurora job name</td><tbody><tr><td>twister2.worker.cpu</td><td>1.0</td><td>number of cores for each worker<br/>it is a floating point number<br/>each worker can have fractional cores such as 0.5 cores or multiple cores as 2<br/>default value is 1.0 core</td><tbody><tr><td>twister2.worker.ram</td><td>200</td><td>amount of memory for each worker in the job in mega bytes as integer<br/>default value is 200 MB</td><tbody><tr><td>twister2.worker.disk</td><td>1024</td><td>amount of hard disk space on each worker in mega bytes<br/>this only used when running twister2 in Aurora<br/>default value is 1024 MB.</td><tbody><tr><td>twister2.worker.instances</td><td>6</td><td>number of worker instances</td></tbody></table>

### Aurora Core Configurations



No specific configurations
### Aurora Task Configurations



No specific configurations
## Kubernetes configurations
### Kubernetes Checkpoint Configurations



No specific configurations
### Kubernetes Data Configurations



No specific configurations
### Kubernetes Network Configurations



#### OpenMPI settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.network.channel.class</td><td>"edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"</td><td>If the channel is set as TWSMPIChannel,<br/>the job is started as OpenMPI job<br/>Otherwise, it is a regular twister2 job. OpenMPI is not started in this case.</td><tbody><tr><td>kubernetes.secret.name</td><td>"twister2-openmpi-ssh-key"</td><td>A Secret object must be present in Kubernetes master<br/>Its name must be specified here</td></tbody></table>

#### Worker port settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>kubernetes.worker.base.port</td><td>9000</td><td>the base port number workers will use internally to communicate with each other<br/>when there are multiple workers in a pod, first worker will get this port number,<br/>second worker will get the next port, and so on.<br/>default value is 9000,</td><tbody><tr><td>kubernetes.worker.transport.protocol</td><td>"TCP"</td><td>transport protocol for the worker. TCP or UDP<br/>by default, it is TCP<br/>set if it is UDP</td></tbody></table>

#### NodePort service parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>kubernetes.node.port.service.requested</td><td>true</td><td>if the job requests NodePort service, it must be true<br/>NodePort service makes the workers accessible from external entities (outside of the cluster)<br/>by default, its value is false</td><tbody><tr><td>kubernetes.service.node.port</td><td>30003</td><td>if NodePort value is 0, it is automatically assigned a value<br/>the user can request a specific port value in the NodePort range by setting the value below<br/>by default Kubernetes uses the range 30000-32767 for NodePorts<br/>Kubernetes admins can change this range</td></tbody></table>

### Kubernetes Resource Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.docker.image.for.kubernetes</td><td>"twister2/twister2-k8s:0.3.0"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"twister2/twister2-k8s:0.3.0-au"</td></tr></tbody></table><td>Twister2 Docker image for Kubernetes</td><tbody><tr><td>twister2.system.package.uri</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><td>the package uri</td><tbody><tr><td>twister2.class.launcher</td><td>edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesLauncher</td><td></td><tbody><tr><td>kubernetes.image.pull.policy</td><td>"Always"</td><td>image pull policy, by default is IfNotPresent<br/>it could also be Always</td></tbody></table>

#### Kubernetes Mapping and Binding parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>kubernetes.bind.worker.to.cpu</td><td>true</td><td>Statically bind workers to CPUs<br/>Workers do not move from the CPU they are started during computation<br/>twister2.cpu_per_container has to be an integer<br/>by default, its value is false</td><tbody><tr><td>kubernetes.worker.to.node.mapping</td><td>true</td><td>kubernetes can map workers to nodes as specified by the user<br/>default value is false</td><tbody><tr><td>kubernetes.worker.mapping.key</td><td>"kubernetes.io/hostname"</td><td>the label key on the nodes that will be used to map workers to nodes</td><tbody><tr><td>kubernetes.worker.mapping.operator</td><td>"In"</td><td>operator to use when mapping workers to nodes based on key value<br/>Exists/DoesNotExist checks only the existence of the specified key in the node.<br/>Ref https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#node-affinity-beta-feature</td><tbody><tr><td>kubernetes.worker.mapping.values</td><td>['e012', 'e013']</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>[]</td></tr></tbody></table><td>values for the mapping key<br/>when the mapping operator is either Exists or DoesNotExist, values list must be empty.</td><tbody><tr><td>Valid values</td><td>all-same-node, all-separate-nodes, none</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"all-same-node"</td></tr></tbody></table><td>uniform worker mapping<br/>default value is none</td></tbody></table>

#### ZooKeeper related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.zookeeper.server.addresses</td><td>"ip:port"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td></tr></tbody></table><td></td><tbody><tr><td>twister2.zookeeper.root.node.path</td><td>"/twister2"</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td></tbody></table>

#### When a job is submitted, the job package needs to be transferred to worker pods<br/>Two upload methods are provided:<br/>  a) Job Package Transfer Using kubectl file copy (default)<br/>  b) Job Package Transfer Through a Web Server<br/>Following two configuration parameters control the uploading with the first method<br/>when the submitting client uploads the job package directly to pods using kubectl copy
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.kubernetes.client.to.pods.uploading</td><td>true</td><td>if the value of this parameter is true,<br/>the job package is transferred from submitting client to pods directly<br/>if it is false, the job package will be transferred to pods through the upload web server<br/>default value is true</td><tbody><tr><td>twister2.kubernetes.uploader.watch.pods.starting</td><td>true</td><td>When the job package is transferred from submitting client to pods directly,<br/>upload attempts can either start after watching pods or immediately when StatefulSets are created<br/>watching pods before starting file upload attempts is more accurate<br/>it may be slightly slower to transfer the job package by watching pods though<br/>default value is true</td></tbody></table>

#### Following configuration parameters sets up the upload web server,<br/>when the job package is transferred through a webserver<br/>Workers download the job package from this web server
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.uploader.directory</td><td>"/absolute/path/to/uploder/directory/"</td><td>the directory where the job package file will be uploaded,<br/>make sure the user has the necessary permissions to upload the file there.<br/>full directory path to upload the job package with scp</td><tbody><tr><td>twister2.uploader.directory.repository</td><td>"/absolute/path/to/uploder/directory/"</td><td></td><tbody><tr><td>twister2.download.directory</td><td>"http://webserver.address:port/download/dir/path"</td><td>web server link of the job package download directory<br/>this is the same directory as the uploader directory</td><tbody><tr><td>twister2.uploader.scp.command.options</td><td>"--chmod=+rwx"</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td><tbody><tr><td>twister2.uploader.scp.command.connection</td><td>"user@uploadserver.address"</td><td>The scp connection string sets the remote user name and host used by the uploader.</td><tbody><tr><td>twister2.uploader.ssh.command.options</td><td>""</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td><tbody><tr><td>twister2.uploader.ssh.command.connection</td><td>"user@uploadserver.address"</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td><tbody><tr><td>twister2.class.uploader</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"</td></tr><tr><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td></tr></tbody></table><td>the uploader class</td><tbody><tr><td>twister2.uploader.download.method</td><td>"HTTP"</td><td>this is the method that workers use to download the core and job packages<br/>it could be  HTTP, HDFS, ..</td></tbody></table>

#### Job configuration parameters for submission of twister2 jobs
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.job.name</td><td>"t2-job"</td><td>twister2 job name</td><tbody><tr><td>    # number of workers using this compute resource</td><td>instances * workersPerPod</td><td>A Twister2 job can have multiple sets of compute resources<br/>instances shows the number of compute resources to be started with this specification<br/>workersPerPod shows the number of workers on each pod in Kubernetes.<br/>   May be omitted in other clusters. default value is 1.</td><tbody><tr><td>#- cpu</td><td>0.5  # number of cores for each worker, may be fractional such as 0.5 or 2.4</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>1024 # ram for each worker as Mega bytes</td></tr><tr><td>1.0 # volatile disk for each worker as Giga bytes</td></tr><tr><td>2 # number of compute resource instances with this specification</td></tr><tr><td>false # only one ComputeResource can be scalable in a job</td></tr><tr><td>1 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td></tr></tbody></table><td></td><tbody><tr><td>twister2.job.driver.class</td><td>"edu.iu.dsc.tws.examples.internal.rsched.DriverExample"</td><td>driver class to run</td><tbody><tr><td>twister2.job.worker.class</td><td>"edu.iu.dsc.tws.examples.internal.rsched.BasicK8sWorker"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"edu.iu.dsc.tws.examples.internal.comms.BroadcastCommunication"</td></tr><tr><td>"edu.iu.dsc.tws.examples.batch.sort.SortJob"</td></tr><tr><td>"edu.iu.dsc.tws.examples.internal.BasicNetworkTest"</td></tr><tr><td>"edu.iu.dsc.tws.examples.comms.batch.BReduceExample"</td></tr><tr><td>"edu.iu.dsc.tws.examples.internal.BasicNetworkTest"</td></tr><tr><td>"edu.iu.dsc.tws.examples.batch.comms.batch.BReduceExample"</td></tr></tbody></table><td>worker class to run</td><tbody><tr><td>twister2.worker.additional.ports</td><td>["port1", "port2", "port3"]</td><td>by default each worker has one port<br/>additional ports can be requested for all workers in a job<br/>please provide the requested port names as a list</td></tbody></table>

#### Kubernetes related settings<br/>namespace to use in kubernetes<br/>default value is "default"
#### Node locations related settings<br/>If this parameter is set as true,<br/>Twister2 will use the below lists for node locations:<br/>  kubernetes.datacenters.list<br/>  kubernetes.racks.list<br/>Otherwise, it will try to get these information by querying Kubernetes Master<br/>It will use below two labels when querying node locations<br/>For this to work, submitting client has to have admin privileges
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>rack.labey.key</td><td>rack</td><td>rack label key for Kubernetes nodes in a cluster<br/>each rack should have a unique label<br/>all nodes in a rack should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td><tbody><tr><td>datacenter.labey.key</td><td>datacenter</td><td>data center label key<br/>each data center should have a unique label<br/>all nodes in a data center should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td><tbody><tr><td>  - echo</td><td>['blue-rack', 'green-rack']</td><td>Data center list with rack names</td><tbody><tr><td>  - green-rack</td><td>['node11.ip', 'node12.ip', 'node13.ip']</td><td>Rack list with node IPs in them</td></tbody></table>

#### persistent volume related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>persistent.volume.per.worker</td><td>0.0</td><td>persistent volume size per worker in GB as double<br/>default value is 0.0Gi<br/>set this value to zero, if you have not persistent disk support<br/>when this value is zero, twister2 will not try to set up persistent storage for this job</td><tbody><tr><td>kubernetes.persistent.storage.class</td><td>"twister2-nfs-storage"</td><td>the admin should provide a PersistentVolume object with the following storage class.<br/>Default storage class name is "twister2".</td><tbody><tr><td>kubernetes.storage.access.mode</td><td>"ReadWriteMany"</td><td>persistent storage access mode.<br/>It shows the access mode for workers to access the shared persistent storage.<br/>if it is "ReadWriteMany", many workers can read and write<br/>https://kubernetes.io/docs/concepts/storage/persistent-volumes</td></tbody></table>

### Kubernetes Core Configurations



#### Logging related settings<br/>for Twister2 workers
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.logging.level</td><td>"INFO"</td><td>default value is INFO</td><tbody><tr><td>persistent.logging.requested</td><td>true</td><td>Do workers request persistent logging? it could be true or false<br/>default value is false</td><tbody><tr><td>twister2.logging.redirect.sysouterr</td><td>false</td><td>whether System.out and System.err should be redircted to log files<br/>When System.out and System.err are redirected to log file, <br/>All messages are only saved in log files. Only a few intial messages are shown on Dashboard.<br/>Otherwise, Dashboard has the complete messages,<br/>log files has the log messages except System.out and System.err.</td><tbody><tr><td>twister2.logging.max.file.size.mb</td><td>100</td><td>The maximum log file size in MB</td><tbody><tr><td>twister2.logging.maximum.files</td><td>5</td><td>The maximum number of log files for each worker</td></tbody></table>

#### Twister2 Job Master related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.job.master.runs.in.client</td><td>false</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster <br/>by default, it is true<br/>when the job master runs in the submitting client,<br/>this client has to be submitting the job from a machine in the cluster<br/>getLocalHost must return a reachable IP address to the job master</td><tbody><tr><td>twister2.job.master.assigns.worker.ids</td><td>false</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when registering with the job master<br/>it is always false in Kubernetes<br/>setting it to true does not make any difference</td><tbody><tr><td>twister2.worker.ping.interval</td><td>10000</td><td>ping message intervals from workers to the job master in milliseconds<br/>default value is 10seconds = 10000</td><tbody><tr><td>twister2.job.master.port</td><td>11011</td><td>twister2 job master port number<br/>default value is 11011</td><tbody><tr><td>twister2.worker.to.job.master.response.wait.duration</td><td>10000</td><td>worker to job master response wait time in milliseconds<br/>this is for messages that wait for a response from the job master<br/>default value is 10seconds = 10000</td><tbody><tr><td>twister2.job.master.volatile.volume.size</td><td>0.0</td><td>twister2 job master volatile volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, volatile volume is not setup for job master</td><tbody><tr><td>twister2.job.master.persistent.volume.size</td><td>0.0</td><td>twister2 job master persistent volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, persistent volume is not setup for job master</td><tbody><tr><td>twister2.job.master.cpu</td><td>0.2</td><td>twister2 job master cpu request<br/>default value is 0.2 percentage</td><tbody><tr><td>twister2.job.master.ram</td><td>1024</td><td>twister2 job master RAM request in MB<br/>default value is 1024 MB</td></tbody></table>

#### WorkerController related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.worker.controller.max.wait.time.for.all.workers.to.join</td><td>100000</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td><tbody><tr><td>twister2.worker.controller.max.wait.time.on.barrier</td><td>100000</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></tbody></table>

#### Dashboard related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.dashboard.host</td><td>"http://twister2-dashboard.default.svc.cluster.local"</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard<br/>if dashboard is running as a statefulset in the cluster</td></tbody></table>

### Kubernetes Task Configurations



No specific configurations
## Mesos configurations
### Mesos Checkpoint Configurations



No specific configurations
### Mesos Data Configurations



No specific configurations
### Mesos Network Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.network.channel.class</td><td>"edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"</td><td></td></tbody></table>

### Mesos Resource Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.scheduler.mesos.scheduler.working.directory</td><td>"~/.twister2/repository"#"${TWISTER2_DIST}/topologies/${CLUSTER}/${ROLE}/${TOPOLOGY}"</td><td>working directory for the topologies</td><tbody><tr><td>twister2.directory.core-package</td><td>"/root/.twister2/repository/twister2-core/"</td><td></td><tbody><tr><td>twister2.directory.sandbox.java.home</td><td>"${JAVA_HOME}"</td><td>location of java - pick it up from shell environment</td><tbody><tr><td>#twister2.mesos.master.uri</td><td>"149.165.150.81:5050"     #######don't forget to uncomment when pushing####################</td><td>The URI of Mesos Master</td><tbody><tr><td>twister2.mesos.framework.name</td><td>"Twister2 framework"</td><td>mesos framework name</td><tbody><tr><td>twister2.mesos.master.uri</td><td>"zk://localhost:2181/mesos"</td><td></td><tbody><tr><td>twister2.mesos.framework.staging.timeout.ms</td><td>2000</td><td>The maximum time in milliseconds waiting for MesosFramework got registered with Mesos Master</td><tbody><tr><td>twister2.mesos.scheduler.driver.stop.timeout.ms</td><td>5000</td><td>The maximum time in milliseconds waiting for Mesos Scheduler Driver to complete stop()</td><tbody><tr><td>twister2.mesos.native.library.path</td><td>"/usr/lib/mesos/0.28.1/lib/"</td><td>the path to load native mesos library</td><tbody><tr><td>twister2.system.package.uri</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><td>the core package uri</td><tbody><tr><td>twister2.mesos.overlay.network.name</td><td>"mesos-overlay"</td><td></td><tbody><tr><td>twister2.docker.image.name</td><td>"gurhangunduz/twister2-mesos:docker-mpi"</td><td></td><tbody><tr><td>twister2.system.job.uri</td><td>"http://localhost:8082/twister2/mesos/twister2-job.tar.gz"</td><td>the job package uri for mesos agent to fetch.<br/>For fetching http server must be running on mesos master</td><tbody><tr><td>twister2.class.launcher</td><td>"edu.iu.dsc.tws.rsched.schedulers.mesos.MesosLauncher"</td><td>launcher class for mesos submission</td><tbody><tr><td>twister2.job.worker.class</td><td>"edu.iu.dsc.tws.examples.internal.comms.BroadcastCommunication"</td><td>container class to run in workers</td><tbody><tr><td>twister2.class.mesos.worker</td><td>"edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorker"</td><td>the Mesos worker class</td></tbody></table>

#### ZooKeeper related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.zookeeper.server.addresses</td><td>"localhost:2181"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td></tr></tbody></table><td></td><tbody><tr><td>#twister2.zookeeper.root.node.path</td><td>"/twister2"</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td><tbody><tr><td>twister2.uploader.directory</td><td>"/var/www/html/twister2/mesos/"</td><td>the directory where the file will be uploaded, make sure the user has the necessary permissions<br/>to upload the file here.</td><tbody><tr><td>#twister2.uploader.directory.repository</td><td>"/var/www/html/twister2/mesos/"</td><td></td><tbody><tr><td>twister2.uploader.scp.command.options</td><td>"--chmod=+rwx"</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td><tbody><tr><td>twister2.uploader.scp.command.connection</td><td>"root@149.165.150.81"</td><td>The scp connection string sets the remote user name and host used by the uploader.</td><tbody><tr><td>twister2.uploader.ssh.command.options</td><td>""</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td><tbody><tr><td>twister2.uploader.ssh.command.connection</td><td>"root@149.165.150.81"</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td><tbody><tr><td>twister2.class.uploader</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"</td></tr><tr><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td></tr></tbody></table><td>the uploader class</td><tbody><tr><td>twister2.uploader.download.method</td><td>"HTTP"</td><td>this is the method that workers use to download the core and job packages<br/>it could be  HTTP, HDFS, ..</td><tbody><tr><td>twister2.HTTP.fetch.uri</td><td>"http://149.165.150.81:8082"</td><td>HTTP fetch uri</td></tbody></table>

#### Client configuration parameters for submission of twister2 jobs
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.resource.scheduler.mesos.cluster</td><td>"example"</td><td>cluster name mesos scheduler runs in</td><tbody><tr><td>twister2.resource.scheduler.mesos.role</td><td>"www-data"</td><td>role in cluster</td><tbody><tr><td>twister2.resource.scheduler.mesos.env</td><td>"devel"</td><td>environment name</td><tbody><tr><td>twister2.job.name</td><td>"basic-mesos"</td><td>mesos job name</td><tbody><tr><td>  #  workersPerPod</td><td>2 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td><td>A Twister2 job can have multiple sets of compute resources<br/>instances shows the number of compute resources to be started with this specification<br/>workersPerPod shows the number of workers on each pod in Kubernetes.<br/>   May be omitted in other clusters. default value is 1.</td><tbody><tr><td>    instances</td><td>4 # number of compute resource instances with this specification</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>2 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td></tr></tbody></table><td></td><tbody><tr><td>twister2.worker.additional.ports</td><td>["port1", "port2", "port3"]</td><td>by default each worker has one port<br/>additional ports can be requested for all workers in a job<br/>please provide the requested port names as a list</td><tbody><tr><td>twister2.job.driver.class</td><td>"edu.iu.dsc.tws.examples.internal.rsched.DriverExample"</td><td>driver class to run</td><tbody><tr><td>nfs.server.address</td><td>"149.165.150.81"</td><td>nfs server address</td><tbody><tr><td>nfs.server.path</td><td>"/nfs/shared-mesos/twister2"</td><td>nfs server path</td><tbody><tr><td>twister2.worker_port</td><td>"31000"</td><td>worker port</td><tbody><tr><td>twister2.desired_nodes</td><td>"all"</td><td>desired nodes</td><tbody><tr><td>twister2.use_docker_container</td><td>"true"</td><td></td><tbody><tr><td>rack.labey.key</td><td>rack</td><td>rack label key for Mesos nodes in a cluster<br/>each rack should have a unique label<br/>all nodes in a rack should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td><tbody><tr><td>datacenter.labey.key</td><td>datacenter</td><td>data center label key<br/>each data center should have a unique label<br/>all nodes in a data center should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td><tbody><tr><td>  - echo</td><td>['blue-rack', 'green-rack']</td><td>Data center list with rack names</td><tbody><tr><td>  - blue-rack</td><td>['10.0.0.40', '10.0.0.41', '10.0.0.42', '10.0.0.43', '10.0.0.44', ]</td><td>Rack list with node IPs in them</td></tbody></table>

### Mesos Core Configurations



#### Logging related settings for Twister2 workers
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.logging.level</td><td>"INFO"</td><td>default value is INFO</td><tbody><tr><td>persistent.logging.requested</td><td>true</td><td>Do workers request persistent logging? it could be true or false<br/>default value is false</td><tbody><tr><td>twister2.logging.redirect.sysouterr</td><td>true</td><td>whether System.out and System.err should be redircted to log files<br/>When System.out and System.err are redirected to log file,<br/>All messages are only saved in log files. Only a few intial messages are shown on Dashboard.<br/>Otherwise, Dashboard has the complete messages,<br/>log files has the log messages except System.out and System.err.</td><tbody><tr><td>twister2.logging.max.file.size.mb</td><td>100</td><td>The maximum log file size in MB</td><tbody><tr><td>twister2.logging.maximum.files</td><td>5</td><td>The maximum number of log files for each worker</td></tbody></table>

#### Twister2 Job Master related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.job.master.runs.in.client</td><td>false</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster<br/>by default, it is true<br/>when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td><tbody><tr><td>twister2.job.master.assigns.worker.ids</td><td>false</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when regitering with the job master</td><tbody><tr><td>twister2.worker.ping.interval</td><td>10000</td><td>ping message intervals from workers to the job master in milliseconds<br/>default value is 10seconds = 10000</td><tbody><tr><td>#twister2.job.master.port</td><td>2023</td><td>twister2 job master port number<br/>default value is 11111</td><tbody><tr><td>twister2.worker.to.job.master.response.wait.duration</td><td>10000</td><td>worker to job master response wait time in milliseconds<br/>this is for messages that wait for a response from the job master<br/>default value is 10seconds = 10000</td><tbody><tr><td>twister2.job.master.volatile.volume.size</td><td>1.0</td><td>twister2 job master volatile volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, volatile volume is not setup for job master</td><tbody><tr><td>twister2.job.master.persistent.volume.size</td><td>1.0</td><td>twister2 job master persistent volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, persistent volume is not setup for job master</td><tbody><tr><td>twister2.job.master.cpu</td><td>0.2</td><td>twister2 job master cpu request<br/>default value is 0.2 percentage</td><tbody><tr><td>twister2.job.master.ram</td><td>1000</td><td>twister2 job master RAM request in MB<br/>default value is 0.2 percentage</td><tbody><tr><td>twister2.job.master.ip</td><td>"149.165.150.81"</td><td></td></tbody></table>

#### WorkerController related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.worker.controller.max.wait.time.for.all.workers.to.join</td><td>100000</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td><tbody><tr><td>twister2.worker.controller.max.wait.time.on.barrier</td><td>100000</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></tbody></table>

#### Dashboard related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.dashboard.host</td><td>"http://localhost:8080"</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></tbody></table>

### Mesos Task Configurations



No specific configurations
## Nomad configurations
### Nomad Checkpoint Configurations



No specific configurations
### Nomad Data Configurations



No specific configurations
### Nomad Network Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.network.channel.class</td><td>"edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"</td><td></td></tbody></table>

### Nomad Resource Configurations



<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.resource.scheduler.mpi.working.directory</td><td>"${HOME}/.twister2/jobs"</td><td>working directory</td><tbody><tr><td>twister2.job.package.url</td><td>"http://localhost:8082/twister2/mesos/twister2-job.tar.gz"</td><td></td><tbody><tr><td>twister2.core.package.url</td><td>"http://localhost:8082/twister2/mesos/twister2-core-0.2.2.tar.gz"</td><td></td><tbody><tr><td>twister2.class.launcher</td><td>"edu.iu.dsc.tws.rsched.schedulers.nomad.NomadLauncher"</td><td>the launcher class</td><tbody><tr><td>#twister2.nomad.scheduler.uri</td><td>"http://149.165.150.81:4646"</td><td>The URI of Nomad API</td><tbody><tr><td>twister2.nomad.scheduler.uri</td><td>"http://127.0.0.1:4646"</td><td></td><tbody><tr><td>twister2.nomad.core.freq.mapping</td><td>2000</td><td>The nomad schedules cpu resources in terms of clock frequency (e.g. MHz), while Heron topologies<br/>specify cpu requests in term of cores.  This config maps core to clock freqency.</td><tbody><tr><td>twister2.filesystem.shared</td><td>true</td><td>weather we are in a shared file system, if that is the case, each worker will not download the<br/>core package and job package, otherwise they will download those packages</td><tbody><tr><td>twister2.nomad.shell.script</td><td>"nomad.sh"</td><td>name of the script</td><tbody><tr><td>twister2.system.package.uri</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><td>path to the system core package</td><tbody><tr><td>twister2.zookeeper.server.addresses</td><td>"localhost:2181"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td></tr></tbody></table><td></td><tbody><tr><td>twister2.uploader.directory</td><td>"/tmp/repository"</td><td>the directory where the file will be uploaded, make sure the user has the necessary permissions<br/>to upload the file here.<br/>if you want to run it on a local machine use this value</td><tbody><tr><td>#twister2.uploader.directory</td><td>"/var/www/html/twister2/mesos/"</td><td>if you want to use http server on echo</td><tbody><tr><td>twister2.uploader.directory.repository</td><td>"/var/www/html/twister2/mesos/"</td><td></td><tbody><tr><td>twister2.uploader.scp.command.options</td><td>"--chmod=+rwx"</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td><tbody><tr><td>twister2.uploader.scp.command.connection</td><td>"root@localhost"</td><td>The scp connection string sets the remote user name and host used by the uploader.</td><tbody><tr><td>twister2.uploader.ssh.command.options</td><td>""</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td><tbody><tr><td>twister2.uploader.ssh.command.connection</td><td>"root@localhost"</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td><tbody><tr><td>twister2.class.uploader</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td></tr></tbody></table><td>file system uploader to be used</td><tbody><tr><td>twister2.uploader.download.method</td><td>"LOCAL"</td><td>this is the method that workers use to download the core and job packages<br/>it could be  LOCAL,  HTTP, HDFS, ..</td></tbody></table>

#### client related configurations for job submit
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>nfs.server.address</td><td>"localhost"</td><td>nfs server address</td><tbody><tr><td>nfs.server.path</td><td>"/nfs/shared/twister2"</td><td>nfs server path</td><tbody><tr><td>rack.labey.key</td><td>rack</td><td>rack label key for Mesos nodes in a cluster<br/>each rack should have a unique label<br/>all nodes in a rack should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td><tbody><tr><td>datacenter.labey.key</td><td>datacenter</td><td>data center label key<br/>each data center should have a unique label<br/>all nodes in a data center should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td><tbody><tr><td>  - echo</td><td>['blue-rack', 'green-rack']</td><td>Data center list with rack names</td><tbody><tr><td>  - green-rack</td><td>['node11.ip', 'node12.ip', 'node13.ip']</td><td>Rack list with node IPs in them</td><tbody><tr><td>  #  workersPerPod</td><td>2 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td><td>A Twister2 job can have multiple sets of compute resources<br/>instances shows the number of compute resources to be started with this specification<br/>workersPerPod shows the number of workers on each pod in Kubernetes.<br/>   May be omitted in other clusters. default value is 1.</td><tbody><tr><td>    instances</td><td>4 # number of compute resource instances with this specification</td><table><thead><tr><td>Options</td></tr></thead><tbody><tr><td>2 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td></tr></tbody></table><td></td><tbody><tr><td>twister2.worker.additional.ports</td><td>["port1", "port2", "port3"]</td><td>by default each worker has one port<br/>additional ports can be requested for all workers in a job<br/>please provide the requested port names as a list</td><tbody><tr><td>twister2.worker_port</td><td>"31000"</td><td>worker port</td></tbody></table>

### Nomad Core Configurations



#### Logging related settings<br/>for Twister2 workers
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.logging.level</td><td>"INFO"</td><td>logging level, FINEST, FINER, FINE, CONFIG, INFO, WARNING, SEVERE</td><tbody><tr><td>persistent.logging.requested</td><td>true</td><td>Do workers request persistent logging? it could be true or false<br/>default value is false</td><tbody><tr><td>twister2.logging.redirect.sysouterr</td><td>true</td><td>whether System.out and System.err should be redirected to log files<br/>When System.out and System.err are redirected to log file,<br/>All messages are only saved in log files. Only a few initial messages are shown on Dashboard.<br/>Otherwise, Dashboard has the complete messages,<br/>log files has the log messages except System.out and System.err.</td><tbody><tr><td>twister2.logging.max.file.size.mb</td><td>100</td><td>The maximum log file size in MB</td><tbody><tr><td>twister2.logging.maximum.files</td><td>5</td><td>The maximum number of log files for each worker</td><tbody><tr><td>twister2.logging.sandbox.logging</td><td>true</td><td></td></tbody></table>

#### Twister2 Job Master related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.job.master.runs.in.client</td><td>true</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster<br/>by default, it is true<br/>when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td><tbody><tr><td>twister2.job.master.assigns.worker.ids</td><td>false</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when registering with the job master</td><tbody><tr><td>twister2.worker.ping.interval</td><td>10000</td><td>ping message intervals from workers to the job master in milliseconds<br/>default value is 10seconds = 10000</td><tbody><tr><td>twister2.job.master.port</td><td>11011</td><td>twister2 job master port number<br/>default value is 11111</td><tbody><tr><td>twister2.worker.to.job.master.response.wait.duration</td><td>10000</td><td>worker to job master response wait time in milliseconds<br/>this is for messages that wait for a response from the job master<br/>default value is 10seconds = 10000</td><tbody><tr><td>twister2.job.master.volatile.volume.size</td><td>1.0</td><td>twister2 job master volatile volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, volatile volume is not setup for job master</td><tbody><tr><td>twister2.job.master.persistent.volume.size</td><td>1.0</td><td>twister2 job master persistent volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, persistent volume is not setup for job master</td><tbody><tr><td>twister2.job.master.cpu</td><td>0.2</td><td>twister2 job master cpu request<br/>default value is 0.2 percentage</td><tbody><tr><td>twister2.job.master.ram</td><td>1000</td><td>twister2 job master RAM request in MB<br/>default value is 0.2 percentage</td><tbody><tr><td>twister2.job.master.ip</td><td>"localhost"</td><td>the job master ip to be used, this is used only in client based masters</td></tbody></table>

#### WorkerController related config parameters
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.worker.controller.max.wait.time.for.all.workers.to.join</td><td>1000000</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td><tbody><tr><td>twister2.worker.controller.max.wait.time.on.barrier</td><td>1000000</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></tbody></table>

#### Dashboard related settings
<table><thead><tr><td>Name</td><td>Default</td><td>Description</td></tr></thead><tbody><tr><td>twister2.dashboard.host</td><td>"http://localhost:8080"</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></tbody></table>

### Nomad Task Configurations



No specific configurations
