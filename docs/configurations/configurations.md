# Twister2 Configurations
## Common configurations
### Common Checkpoint Configurations



**twister2.checkpointing.enable**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>Enable or disable checkpointing</td></table>

**twister2.checkpointing.store**
<table><tr><td>default</td><td>edu.iu.dsc.tws.checkpointing.stores.LocalFileStateStore</td><tr><td>description</td><td>The implementation of the store to be used</td></table>

**twister2.checkpointing.store.fs.dir**
<table><tr><td>default</td><td>"${TWISTER2_HOME}/persistent/"</td><tr><td>description</td><td>Root directory of local file system based store</td></table>

**twister2.checkpointing.store.hdfs.dir**
<table><tr><td>default</td><td>"/twister2/persistent/"</td><tr><td>description</td><td>Root directory of hdfs based store</td></table>

**twister2.checkpointing.source.frequency**
<table><tr><td>default</td><td>1000</td><tr><td>description</td><td>Source triggering frequency</td></table>

### Common Data Configurations



**twister2.data.hadoop.home**
<table><tr><td>default</td><td>"${HADOOP_HOME}"</td><tr><td>description</td><td></td></table>

**twister2.data.hdfs.url**
<table><tr><td>default</td><td>"hdfs://namenode:9000"</td><tr><td>description</td><td></td></table>

**twister2.data.hdfs.class**
<table><tr><td>default</td><td>"org.apache.hadoop.hdfs.DistributedFileSystem"</td><tr><td>description</td><td></td></table>

**twister2.data.hdfs.implementation.key**
<table><tr><td>default</td><td>"fs.hdfs.impl"</td><tr><td>description</td><td></td></table>

**twister2.data.hdfs.config.directory**
<table><tr><td>default</td><td>"${HADOOP_HOME}/etc/hadoop/core-site.xml"</td><tr><td>description</td><td></td></table>

**twister2.data.hdfs.data.directory**
<table><tr><td>default</td><td>"/user/username/"</td><tr><td>description</td><td></td></table>

**twister2.data.hdfs.namenode**
<table><tr><td>default</td><td>"namenode.domain.name"</td><tr><td>description</td><td></td></table>

**twister2.data.hdfs.namenode.port**
<table><tr><td>default</td><td>"9000"</td><tr><td>description</td><td></td></table>

### Common Network Configurations



**twister2.network.buffer.size**
<table><tr><td>default</td><td>1024000</td><tr><td>description</td><td>the buffer size to be used</td></table>

**twister2.network.sendBuffer.count**
<table><tr><td>default</td><td>4</td><tr><td>description</td><td>number of send buffers to be used</td></table>

**twister2.network.receiveBuffer.count**
<table><tr><td>default</td><td>4</td><tr><td>description</td><td>number of receive buffers to be used</td></table>

**twister2.network.channel.pending.size**
<table><tr><td>default</td><td>2048</td><tr><td>description</td><td>channel pending messages</td></table>

**twister2.network.send.pending.max**
<table><tr><td>default</td><td>4</td><tr><td>description</td><td>the send pending messages</td></table>

**twister2.network.partition.message.group.low_water_mark**
<table><tr><td>default</td><td>8000</td><tr><td>description</td><td>group up to 8 ~ 16 messages</td></table>

**twister2.network.partition.message.group.high_water_mark**
<table><tr><td>default</td><td>16000</td><tr><td>description</td><td>this is the max number of messages to group</td></table>

**twister2.network.partition.batch.grouping.size**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td>in batch partition operations, this value will be used to create mini batches<br/>within partial receivers</td></table>

**twister2.network.ops.persistent.dirs**
<table><tr><td>default</td><td>["${TWISTER2_HOME}/persistent/"]</td><tr><td>description</td><td>For disk based operations, this directory list will be used to persist incoming messages.<br/>This can be used to balance the load between multiple devices, by specifying directory locations<br/>from different devices.</td></table>

**twister2.network.shuffle.memory.bytes.max**
<table><tr><td>default</td><td>102400000</td><tr><td>description</td><td>the maximum amount of bytes kept in memory for operations that goes to disk</td></table>

**twister2.network.shuffle.memory.records.max**
<table><tr><td>default</td><td>102400000</td><tr><td>description</td><td>the maximum number of records kept in memory for operations that goes to dist</td></table>

**twister2.network.shuffle.file.bytes.max**
<table><tr><td>default</td><td>10000000</td><tr><td>description</td><td>size of the shuffle file (10MB default)</td></table>

**twister2.network.shuffle.parallel.io**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>no of parallel IO operations permitted</td></table>

**twister2.network.partition.algorithm.stream**
<table><tr><td>default</td><td>"simple"</td><tr><td>description</td><td>the partitioning algorithm</td></table>

**twister2.network.partition.algorithm.batch**
<table><tr><td>default</td><td>"simple"</td><tr><td>description</td><td>the partitioning algorithm</td></table>

**twister2.network.partition.algorithm.batch.keyed_gather**
<table><tr><td>default</td><td>"simple"</td><tr><td>description</td><td>the partitioning algorithm</td></table>

**ttwister2.network.partition.ring.group.workers**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>ring group worker</td></table>

### Common Resource Configurations



**twister2.client.debug**
<table><tr><td>default</td><td>'-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006'</td><tr><td>description</td><td>use this property to debug the client submitting the job</td></table>

### Common Core Configurations



#### Twister2 Job Master related settings
**twister2.job.master.used**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td></td></table>

**twister2.job.master.runs.in.client**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster<br/>by default, it is true<br/>when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td></table>

**twister2.job.master.assigns.worker.ids**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when regitering with the job master</td></table>

**twister2.worker.to.job.master.response.wait.duration**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td></td></table>

#### WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></table>

#### Common thread pool config parameters
**twister2.common.thread.pool.threads**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>Maximum number of threads to spawn on demand</td></table>

**twister2.common.thread.pool.keepalive**
<table><tr><td>default</td><td>10</td><tr><td>description</td><td>maximum time that excess idle threads will wait for new tasks before terminating</td></table>

#### Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://localhost:8080"</td><tr><td>description</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></table>

### Common Task Configurations



#### Task Scheduler Related Configurations
**twister2.taskscheduler.streaming**
<table><tr><td>default</td><td>"roundrobin"</td><tr><td>description</td><td>Task scheduling mode for the streaming jobs "roundrobin" or "firstfit" or "datalocalityaware" or "userdefined"<br/>By default it is roundrobin mode.</td></table>

**twister2.taskscheduler.streaming.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler"</td><tr><td>description</td><td>Task Scheduler class for the round robin streaming task scheduler</td></table>

**twister2.taskscheduler.streaming.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler"</td><tr><td>description</td><td>Task Scheduler for the Data Locality Aware Streaming Task Scheduler</td></table>

**twister2.taskscheduler.streaming.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.streaming.firstfit.FirstFitStreamingTaskScheduler"</td><tr><td>description</td><td>Task Scheduler for the FirstFit Streaming Task Scheduler</td></table>

**twister2.taskscheduler.streaming.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler"</td><tr><td>description</td><td>Task Scheduler for the userDefined Streaming Task Scheduler</td></table>

**twister2.taskscheduler.batch**
<table><tr><td>default</td><td>"roundrobin"</td><tr><td>description</td><td>Task scheduling mode for the batch jobs "roundrobin" or "datalocalityaware" or "userdefined"<br/>By default it is roundrobin mode.</td></table>

**twister2.taskscheduler.batch.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler"</td><tr><td>description</td><td>Task Scheduler class for the round robin batch task scheduler</td></table>

**twister2.taskscheduler.batch.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.batch.datalocalityaware.DataLocalityBatchTaskScheduler"</td><tr><td>description</td><td>Task Scheduler for the Data Locality Aware Batch Task Scheduler</td></table>

**twister2.taskscheduler.batch.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler"</td><tr><td>description</td><td>Task Scheduler for the userDefined Batch Task Scheduler</td></table>

**twister2.taskscheduler.task.instances**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>Number of task instances to be allocated to each worker/container</td></table>

**twister2.taskscheduler.task.instance.ram**
<table><tr><td>default</td><td>512.0</td><tr><td>description</td><td>Ram value to be allocated to each task instance</td></table>

**twister2.taskscheduler.task.instance.disk**
<table><tr><td>default</td><td>500.0</td><tr><td>description</td><td>Disk value to be allocated to each task instance</td></table>

**twister2.taskscheduler.instance.cpu**
<table><tr><td>default</td><td>2.0</td><tr><td>description</td><td>CPU value to be allocated to each task instancetwister2.task.parallelism</td></table>

**twister2.taskscheduler.container.instance.ram**
<table><tr><td>default</td><td>4096.0</td><tr><td>description</td><td>Default Container Instance Values<br/>Ram value to be allocated to each container</td></table>

**twister2.taskscheduler.container.instance.disk**
<table><tr><td>default</td><td>8000.0</td><tr><td>description</td><td>Disk value to be allocated to each container</td></table>

**twister2.taskscheduler.container.instance.cpu**
<table><tr><td>default</td><td>16.0</td><tr><td>description</td><td></td></table>

**twister2.taskscheduler.ram.padding.container**
<table><tr><td>default</td><td>2.0</td><tr><td>description</td><td>Default Container Padding Values<br/>Default padding value of the ram to be allocated to each container</td></table>

**twister2.taskscheduler.disk.padding.container**
<table><tr><td>default</td><td>12.0</td><tr><td>description</td><td>Default padding value of the disk to be allocated to each container</td></table>

**twister2.taskscheduler.cpu.padding.container**
<table><tr><td>default</td><td>1.0</td><tr><td>description</td><td>CPU padding value to be allocated to each container</td></table>

**twister2.taskscheduler.container.padding.percentage**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>Percentage value to be allocated to each container</td></table>

**twister2.taskscheduler.container.instance.bandwidth**
<table><tr><td>default</td><td>100 #Mbps</td><tr><td>description</td><td>Static Default Network parameters<br/>Bandwidth value to be allocated to each container instance for datalocality scheduling</td></table>

**twister2.taskscheduler.container.instance.latency**
<table><tr><td>default</td><td>0.002 #Milliseconds</td><tr><td>description</td><td>Latency value to be allocated to each container instance for datalocality scheduling</td></table>

**twister2.taskscheduler.datanode.instance.bandwidth**
<table><tr><td>default</td><td>200 #Mbps</td><tr><td>description</td><td>Bandwidth to be allocated to each datanode instance for datalocality scheduling</td></table>

**twister2.taskscheduler.datanode.instance.latency**
<table><tr><td>default</td><td>0.01 #Milliseconds</td><tr><td>description</td><td>Latency value to be allocated to each datanode instance for datalocality scheduling</td></table>

**twister2.taskscheduler.task.parallelism**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>Prallelism value to each task instance</td></table>

**twister2.taskscheduler.task.type**
<table><tr><td>default</td><td>"streaming"</td><tr><td>description</td><td>Task type to each submitted job by default it is "streaming" job.</td></table>

**twister2.exector.worker.threads**
<table><tr><td>default</td><td>1</td><tr><td>description</td><td>number of threads per worker</td></table>

**twister2.executor.batch.name**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.executor.threading.BatchSharingExecutor2"</td><tr><td>description</td><td>name of the batch executor</td></table>

**twister2.exector.instance.queue.low.watermark**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td>number of tuples executed at a single pass</td></table>

## Standalone configurations
### Standalone Checkpoint Configurations



### Standalone Data Configurations



### Standalone Network Configurations



**twister2.network.channel.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.comms.mpi.TWSMPIChannel"</td><tr><td>description</td><td></td></table>

### Standalone Resource Configurations



**twister2.resource.scheduler.mpi.working.directory**
<table><tr><td>default</td><td>"${HOME}/.twister2/jobs"</td><tr><td>description</td><td>working directory</td></table>

**twsiter2.resource.scheduler.mpi.mode**
<table><tr><td>default</td><td>"standalone"</td><tr><td>description</td><td>mode of the mpi scheduler</td></table>

**twister2.resource.scheduler.mpi.job.id**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>the job id file</td></table>

**twister2.resource.scheduler.mpi.shell.script**
<table><tr><td>default</td><td>"mpi.sh"</td><tr><td>description</td><td>slurm script to run</td></table>

**twister2.resource.scheduler.mpi.home**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>the mpirun command location</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><tr><td>description</td><td>the package uri</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher"</td><tr><td>description</td><td>the launcher class</td></table>

**twister2.resource.scheduler.mpi.mpirun.file**
<table><tr><td>default</td><td>"twister2-core/ompi/bin/mpirun"</td><tr><td>description</td><td>mpi run file, this assumes a mpirun that is shipped with the product<br/>change this to just mpirun if you are using a system wide installation of OpenMPI<br/>or complete path of OpenMPI in case you have something custom</td></table>

**twister2.resource.scheduler.mpi.mapby**
<table><tr><td>default</td><td>"node"</td><tr><td>description</td><td>mpi scheduling policy. Two possible options are node and slot.<br/>read more at https://www.open-mpi.org/faq/?category=running#mpirun-scheduling</td></table>

**twister2.resource.scheduler.mpi.mapby.use-pe**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>use mpi map-by modifier PE. If this option is enabled, cpu count of compute resource<br/>specified in job definition will be taken into consideration</td></table>

**twister2.resource.sharedfs**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>Indicates whether bootstrap process needs to be run and distribute job file and core<br/>between MPI nodes. Twister2 assumes job file is accessible to all nodes if this property is set<br/>to true, else it will run the bootstrap process</td></table>

**twister2.resource.fs.mount**
<table><tr><td>default</td><td>"${TWISTER2_HOME}/persistent/fs/"</td><tr><td>description</td><td>Directory for file system volume mount</td></table>

**twister2.uploader.directory**
<table><tr><td>default</td><td>"${HOME}/.twister2/repository"</td><tr><td>description</td><td>the uploader directory</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uplaoder class</td></table>

**twister2.uploader.download.method**
<table><tr><td>default</td><td>"HTTP"</td><tr><td>description</td><td>this is the method that workers use to download the core and job packages<br/>it could be  HTTP, HDFS, ..</td></table>

### Standalone Core Configurations



#### Twister2 Job Master related settings
**twister2.job.master.used**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td></td></table>

**twister2.job.master.runs.in.client**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster<br/>by default, it is true<br/>when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td></table>

**twister2.job.master.assigns.worker.ids**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when regitering with the job master</td></table>

**twister2.worker.to.job.master.response.wait.duration**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td></td></table>

#### WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></table>

#### Common thread pool config parameters
**twister2.common.thread.pool.threads**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>Maximum number of threads to spawn on demand</td></table>

**twister2.common.thread.pool.keepalive**
<table><tr><td>default</td><td>10</td><tr><td>description</td><td>maximum time that excess idle threads will wait for new tasks before terminating</td></table>

#### Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://localhost:8080"</td><tr><td>description</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></table>

### Standalone Task Configurations



## Slurm configurations
### Slurm Checkpoint Configurations



### Slurm Data Configurations



### Slurm Network Configurations



**twister2.network.channel.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.comms.mpi.TWSMPIChannel"</td><tr><td>description</td><td></td></table>

### Slurm Resource Configurations



**twister2.resource.scheduler.mpi.working.directory**
<table><tr><td>default</td><td>"${HOME}/.twister2/jobs"</td><tr><td>description</td><td>working directory</td></table>

**twsiter2.resource.scheduler.mpi.mode**
<table><tr><td>default</td><td>"slurm"</td><tr><td>description</td><td>mode of the mpi scheduler</td></table>

**twister2.resource.scheduler.mpi.job.id**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>the job id file</td></table>

**twister2.resource.scheduler.mpi.shell.script**
<table><tr><td>default</td><td>"mpi.sh"</td><tr><td>description</td><td>slurm script to run</td></table>

**twister2.resource.scheduler.slurm.partition**
<table><tr><td>default</td><td>"juliet"</td><tr><td>description</td><td>slurm partition</td></table>

**twister2.resource.scheduler.mpi.home**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>the mpirun command location</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><tr><td>description</td><td>the package uri</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher"</td><tr><td>description</td><td>the launcher class</td></table>

**twister2.resource.scheduler.mpi.mpirun.file**
<table><tr><td>default</td><td>"twister2-core/ompi/bin/mpirun"</td><tr><td>description</td><td>mpi run file, this assumes a mpirun that is shipped with the product<br/>change this to just mpirun if you are using a system wide installation of OpenMPI<br/>or complete path of OpenMPI in case you have something custom</td></table>

**twister2.uploader.directory**
<table><tr><td>default</td><td>"${HOME}/.twister2/repository"</td><tr><td>description</td><td>the uploader directory</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uplaoder class</td></table>

### Slurm Core Configurations



#### WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></table>

#### Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://localhost:8080"</td><tr><td>description</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></table>

### Slurm Task Configurations



## Aurora configurations
### Aurora Checkpoint Configurations



### Aurora Data Configurations



### Aurora Network Configurations



**twister2.network.channel.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"</td><tr><td>description</td><td></td></table>

### Aurora Resource Configurations



**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><tr><td>description</td><td>the package uri</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraLauncher</td><tr><td>description</td><td>launcher class for aurora submission</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><tr><td>options</td><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"<br/>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uploader class</td></table>

**twister2.job.worker.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.examples.internal.rsched.BasicAuroraContainer"</td><tr><td>description</td><td>container class to run in workers</td></table>

**twister2.class.aurora.worker**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraWorkerStarter"</td><tr><td>description</td><td>the Aurora worker class</td></table>

#### ZooKeeper related config parameters
**twister2.zookeeper.server.addresses**
<table><tr><td>default</td><td>"149.165.150.81:2181"</td><tr><td>options</td><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td><tr><td>description</td><td></td></table>

**#twister2.zookeeper.root.node.path**
<table><tr><td>default</td><td>"/twister2"</td><tr><td>description</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td></table>

**twister2.zookeeper.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>if the workers want to wait for all others to join a job, max wait time in ms</td></table>

#### Uploader configuration
**twister2.uploader.directory**
<table><tr><td>default</td><td>"/root/.twister2/repository/"</td><tr><td>description</td><td>the directory where the file will be uploaded, make sure the user has the necessary permissions<br/>to upload the file here.</td></table>

**twister2.uploader.directory.repository**
<table><tr><td>default</td><td>"/root/.twister2/repository/"</td><tr><td>description</td><td></td></table>

**twister2.uploader.scp.command.options**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td></table>

**twister2.uploader.scp.command.connection**
<table><tr><td>default</td><td>"root@149.165.150.81"</td><tr><td>description</td><td>The scp connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.uploader.ssh.command.options**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td></table>

**twister2.uploader.ssh.command.connection**
<table><tr><td>default</td><td>"root@149.165.150.81"</td><tr><td>description</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td></table>

#### Client configuration parameters for submission of twister2 jobs
**twister2.resource.scheduler.aurora.script**
<table><tr><td>default</td><td>"${TWISTER2_CONF}/twister2.aurora"</td><tr><td>description</td><td>aurora python script to submit a job to Aurora Scheduler<br/>its default value is defined as the following in the code<br/>can be reset from this config file if desired</td></table>

**twister2.resource.scheduler.aurora.cluster**
<table><tr><td>default</td><td>"example"</td><tr><td>description</td><td>cluster name aurora scheduler runs in</td></table>

**twister2.resource.scheduler.aurora.role**
<table><tr><td>default</td><td>"www-data"</td><tr><td>description</td><td>role in cluster</td></table>

**twister2.resource.scheduler.aurora.env**
<table><tr><td>default</td><td>"devel"</td><tr><td>description</td><td>environment name</td></table>

**twister2.job.name**
<table><tr><td>default</td><td>"basic-aurora"</td><tr><td>options</td><td>"basic-aurora"</td><tr><td>description</td><td>aurora job name</td></table>

**twister2.worker.cpu**
<table><tr><td>default</td><td>1.0</td><tr><td>description</td><td>number of cores for each worker<br/>it is a floating point number<br/>each worker can have fractional cores such as 0.5 cores or multiple cores as 2<br/>default value is 1.0 core</td></table>

**twister2.worker.ram**
<table><tr><td>default</td><td>200</td><tr><td>description</td><td>amount of memory for each worker in the job in mega bytes as integer<br/>default value is 200 MB</td></table>

**twister2.worker.disk**
<table><tr><td>default</td><td>1024</td><tr><td>description</td><td>amount of hard disk space on each worker in mega bytes<br/>this only used when running twister2 in Aurora<br/>default value is 1024 MB.</td></table>

**twister2.worker.instances**
<table><tr><td>default</td><td>6</td><tr><td>description</td><td>number of worker instances</td></table>

### Aurora Core Configurations



### Aurora Task Configurations



## Kubernetes configurations
### Kubernetes Checkpoint Configurations



### Kubernetes Data Configurations



### Kubernetes Network Configurations



#### OpenMPI settings
**twister2.network.channel.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"</td><tr><td>description</td><td>If the channel is set as TWSMPIChannel,<br/>the job is started as OpenMPI job<br/>Otherwise, it is a regular twister2 job. OpenMPI is not started in this case.</td></table>

**kubernetes.secret.name**
<table><tr><td>default</td><td>"twister2-openmpi-ssh-key"</td><tr><td>description</td><td>A Secret object must be present in Kubernetes master<br/>Its name must be specified here</td></table>

#### Worker port settings
**kubernetes.worker.base.port**
<table><tr><td>default</td><td>9000</td><tr><td>description</td><td>the base port number workers will use internally to communicate with each other<br/>when there are multiple workers in a pod, first worker will get this port number,<br/>second worker will get the next port, and so on.<br/>default value is 9000,</td></table>

**kubernetes.worker.transport.protocol**
<table><tr><td>default</td><td>"TCP"</td><tr><td>description</td><td>transport protocol for the worker. TCP or UDP<br/>by default, it is TCP<br/>set if it is UDP</td></table>

#### NodePort service parameters
**kubernetes.node.port.service.requested**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>if the job requests NodePort service, it must be true<br/>NodePort service makes the workers accessibale from external entities (outside of the cluster)<br/>by default, its value is false</td></table>

**by default Kubernetes uses the range**
<table><tr><td>default</td><td>30000-32767 for NodePorts</td><tr><td>options</td><td>30003</td><tr><td>description</td><td>if NodePort value is 0, it is automatically assigned a value<br/>the user can request a specific port value in the NodePort range by setting the value below<br/>Kubernetes admins can change this range</td></table>

### Kubernetes Resource Configurations



**twister2.docker.image.for.kubernetes**
<table><tr><td>default</td><td>"twister2/twister2-k8s:0.3.0"</td><tr><td>options</td><td>"twister2/twister2-k8s:0.3.0-au"</td><tr><td>description</td><td>Twister2 Docker image for Kubernetes</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><tr><td>description</td><td>the package uri</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesLauncher</td><tr><td>description</td><td></td></table>

**kubernetes.image.pull.policy**
<table><tr><td>default</td><td>"Always"</td><tr><td>description</td><td>image pull policy, by default is IfNotPresent<br/>it could also be Always</td></table>

#### Kubernetes Mapping and Binding parameters<br/>Statically bind workers to CPUs<br/>Workers do not move from the CPU they are started during computation<br/>twister2.cpu_per_container has to be an integer<br/>by default, its value is false
**kubernetes.worker.to.node.mapping**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>kubernetes can map workers to nodes as specified by the user<br/>default value is false</td></table>

**kubernetes.worker.mapping.key**
<table><tr><td>default</td><td>"kubernetes.io/hostname"</td><tr><td>description</td><td>the label key on the nodes that will be used to map workers to nodes</td></table>

**kubernetes.worker.mapping.operator**
<table><tr><td>default</td><td>"In"</td><tr><td>options</td><td>https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#node-affinity-beta-feature</td><tr><td>description</td><td>operator to use when mapping workers to nodes based on key value<br/>Exists/DoesNotExist checks only the existence of the specified key in the node.</td></table>

**kubernetes.worker.mapping.values**
<table><tr><td>default</td><td>['e012', 'e013']</td><tr><td>options</td><td>[]</td><tr><td>description</td><td>values for the mapping key<br/>when the mapping operator is either Exists or DoesNotExist, values list must be empty.</td></table>

**Valid values**
<table><tr><td>default</td><td>all-same-node, all-separate-nodes, none</td><tr><td>options</td><td>"all-same-node"</td><tr><td>description</td><td>uniform worker mapping<br/>default value is none</td></table>

#### ZooKeeper related config parameters
**twister2.zookeeper.server.addresses**
<table><tr><td>default</td><td>"ip:port"</td><tr><td>options</td><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td><tr><td>description</td><td></td></table>

**#twister2.zookeeper.root.node.path**
<table><tr><td>default</td><td>"/twister2"</td><tr><td>description</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td></table>

#### When a job is submitted, the job package needs to be transferred to worker pods<br/>Two upload methods are provided:<br/>  a) Job Package Transfer Using kubectl file copy (default)<br/>  b) Job Package Transfer Through a Web Server<br/>Following two configuration parameters control the uploading with the first method<br/>when the submitting client uploads the job package directly to pods using kubectl copy
**twister2.kubernetes.client.to.pods.uploading**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>if the value of this parameter is true,<br/>the job package is transferred from submitting client to pods directly<br/>if it is false, the job package will be transferred to pods through the upload web server<br/>default value is true</td></table>

**twister2.kubernetes.uploader.watch.pods.starting**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>When the job package is transferred from submitting client to pods directly,<br/>upload attempts can either start after watching pods or immediately when StatefulSets are created<br/>watching pods before starting file upload attempts is more accurate<br/>it may be slightly slower to transfer the job package by watching pods though<br/>default value is true</td></table>

#### Following configuration parameters sets up the upload web server,<br/>when the job package is transferred through a webserver<br/>Workers download the job package from this web server
**twister2.uploader.directory**
<table><tr><td>default</td><td>"/absolute/path/to/uploder/directory/"</td><tr><td>description</td><td>the directory where the job package file will be uploaded,<br/>make sure the user has the necessary permissions to upload the file there.<br/>full directory path to upload the job package with scp</td></table>

**twister2.uploader.directory.repository**
<table><tr><td>default</td><td>"/absolute/path/to/uploder/directory/"</td><tr><td>description</td><td></td></table>

**twister2.download.directory**
<table><tr><td>default</td><td>"http://webserver.address:port/download/dir/path"</td><tr><td>description</td><td>web server link of the job package download directory<br/>this is the same directory as the uploader directory</td></table>

**twister2.uploader.scp.command.options**
<table><tr><td>default</td><td>"--chmod=+rwx"</td><tr><td>description</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td></table>

**twister2.uploader.scp.command.connection**
<table><tr><td>default</td><td>"user@uploadserver.address"</td><tr><td>description</td><td>The scp connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.uploader.ssh.command.options**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td></table>

**twister2.uploader.ssh.command.connection**
<table><tr><td>default</td><td>"user@uploadserver.address"</td><tr><td>description</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><tr><td>options</td><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"<br/>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uploader class</td></table>

**twister2.uploader.download.method**
<table><tr><td>default</td><td>"HTTP"</td><tr><td>description</td><td>this is the method that workers use to download the core and job packages<br/>it could be  HTTP, HDFS, ..</td></table>

#### Job configuration parameters for submission of twister2 jobs
**twister2.job.name**
<table><tr><td>default</td><td>"t2-job"</td><tr><td>description</td><td>twister2 job name</td></table>

**    # number of workers using this compute resource**
<table><tr><td>default</td><td>instances * workersPerPod</td><tr><td>description</td><td>A Twister2 job can have multiple sets of compute resources<br/>instances shows the number of compute resources to be started with this specification<br/>workersPerPod shows the number of workers on each pod in Kubernetes.<br/>   May be omitted in other clusters. default value is 1.</td></table>

**#- cpu**
<table><tr><td>default</td><td>0.5  # number of cores for each worker, may be fractional such as 0.5 or 2.4</td><tr><td>options</td><td>1024 # ram for each worker as Mega bytes<br/>1.0 # volatile disk for each worker as Giga bytes<br/>2 # number of compute resource instances with this specification<br/>false # only one ComputeResource can be scalable in a job<br/>1 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td><tr><td>description</td><td></td></table>

**twister2.job.driver.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.examples.internal.rsched.DriverExample"</td><tr><td>description</td><td>driver class to run</td></table>

**twister2.job.worker.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.examples.internal.rsched.BasicK8sWorker"</td><tr><td>options</td><td>"edu.iu.dsc.tws.examples.internal.comms.BroadcastCommunication"<br/>"edu.iu.dsc.tws.examples.batch.sort.SortJob"<br/>"edu.iu.dsc.tws.examples.internal.BasicNetworkTest"<br/>"edu.iu.dsc.tws.examples.comms.batch.BReduceExample"<br/>"edu.iu.dsc.tws.examples.internal.BasicNetworkTest"<br/>"edu.iu.dsc.tws.examples.batch.comms.batch.BReduceExample"</td><tr><td>description</td><td>worker class to run</td></table>

**twister2.worker.additional.ports**
<table><tr><td>default</td><td>["port1", "port2", "port3"]</td><tr><td>description</td><td>by default each worker has one port<br/>additional ports can be requested for all workers in a job<br/>please provide the requested port names as a list</td></table>

#### Kubernetes related settings<br/>namespace to use in kubernetes<br/>default value is "default"
#### Node locations related settings<br/>If this parameter is set as true,<br/>Twister2 will use the below lists for node locations:<br/>  kubernetes.datacenters.list<br/>  kubernetes.racks.list<br/>Otherwise, it will try to get these information by querying Kubernetes Master<br/>It will use below two labels when querying node locations<br/>For this to work, submitting client has to have admin privileges
**rack.labey.key**
<table><tr><td>default</td><td>rack</td><tr><td>description</td><td>rack label key for Kubernetes nodes in a cluster<br/>each rack should have a unique label<br/>all nodes in a rack should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td></table>

**datacenter.labey.key**
<table><tr><td>default</td><td>datacenter</td><tr><td>description</td><td>data center label key<br/>each data center should have a unique label<br/>all nodes in a data center should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td></table>

**  - echo**
<table><tr><td>default</td><td>['blue-rack', 'green-rack']</td><tr><td>description</td><td>Data center list with rack names</td></table>

**  - green-rack**
<table><tr><td>default</td><td>['node11.ip', 'node12.ip', 'node13.ip']</td><tr><td>description</td><td>Rack list with node IPs in them</td></table>

#### persistent volume related settings
**persistent.volume.per.worker**
<table><tr><td>default</td><td>0.0</td><tr><td>description</td><td>persistent volume size per worker in GB as double<br/>default value is 0.0Gi<br/>set this value to zero, if you have not persistent disk support<br/>when this value is zero, twister2 will not try to set up persistent storage for this job</td></table>

**kubernetes.persistent.storage.class**
<table><tr><td>default</td><td>"twister2-nfs-storage"</td><tr><td>description</td><td>the admin should provide a PersistentVolume object with the following storage class.<br/>Default storage class name is "twister2".</td></table>

**kubernetes.storage.access.mode**
<table><tr><td>default</td><td>"ReadWriteMany"</td><tr><td>description</td><td>persistent storage access mode.<br/>It shows the access mode for workers to access the shared persistent storage.<br/>if it is "ReadWriteMany", many workers can read and write<br/>https://kubernetes.io/docs/concepts/storage/persistent-volumes</td></table>

### Kubernetes Core Configurations



#### Logging related settings<br/>for Twister2 workers
**twister2.logging.level**
<table><tr><td>default</td><td>"INFO"</td><tr><td>description</td><td>default value is INFO</td></table>

**persistent.logging.requested**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>Do workers request persistent logging? it could be true or false<br/>default value is false</td></table>

**twister2.logging.redirect.sysouterr**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>whether System.out and System.err should be redircted to log files<br/>When System.out and System.err are redirected to log file, <br/>All messages are only saved in log files. Only a few intial messages are shown on Dashboard.<br/>Otherwise, Dashboard has the complete messages,<br/>log files has the log messages except System.out and System.err.</td></table>

**twister2.logging.max.file.size.mb**
<table><tr><td>default</td><td>100</td><tr><td>description</td><td>The maximum log file size in MB</td></table>

**twister2.logging.maximum.files**
<table><tr><td>default</td><td>5</td><tr><td>description</td><td>The maximum number of log files for each worker</td></table>

#### Twister2 Job Master related settings
**twister2.job.master.runs.in.client**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster <br/>by default, it is true<br/>when the job master runs in the submitting client,<br/>this client has to be submitting the job from a machine in the cluster<br/>getLocalHost must return a reachable IP address to the job master</td></table>

**twister2.job.master.assigns.worker.ids**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when registering with the job master<br/>it is always false in Kubernetes<br/>setting it to true does not make any difference</td></table>

**twister2.worker.ping.interval**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td>ping message intervals from workers to the job master in milliseconds<br/>default value is 10seconds = 10000</td></table>

**twister2.job.master.port**
<table><tr><td>default</td><td>11011</td><tr><td>description</td><td>twister2 job master port number<br/>default value is 11011</td></table>

**twister2.worker.to.job.master.response.wait.duration**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td>worker to job master response wait time in milliseconds<br/>this is for messages that wait for a response from the job master<br/>default value is 10seconds = 10000</td></table>

**twister2.job.master.volatile.volume.size**
<table><tr><td>default</td><td>0.0</td><tr><td>description</td><td>twister2 job master volatile volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, volatile volume is not setup for job master</td></table>

**twister2.job.master.persistent.volume.size**
<table><tr><td>default</td><td>0.0</td><tr><td>description</td><td>twister2 job master persistent volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, persistent volume is not setup for job master</td></table>

**twister2.job.master.cpu**
<table><tr><td>default</td><td>0.2</td><tr><td>description</td><td>twister2 job master cpu request<br/>default value is 0.2 percentage</td></table>

**twister2.job.master.ram**
<table><tr><td>default</td><td>1024</td><tr><td>description</td><td>twister2 job master RAM request in MB<br/>default value is 1024 MB</td></table>

#### WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></table>

#### Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://twister2-dashboard.default.svc.cluster.local"</td><tr><td>description</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard<br/>if dashboard is running as a statefulset in the cluster</td></table>

### Kubernetes Task Configurations



## Mesos configurations
### Mesos Checkpoint Configurations



### Mesos Data Configurations



### Mesos Network Configurations



**twister2.network.channel.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"</td><tr><td>description</td><td></td></table>

### Mesos Resource Configurations



**twister2.scheduler.mesos.scheduler.working.directory**
<table><tr><td>default</td><td>"~/.twister2/repository"#"${TWISTER2_DIST}/topologies/${CLUSTER}/${ROLE}/${TOPOLOGY}"</td><tr><td>description</td><td>working directory for the topologies</td></table>

**twister2.directory.core-package**
<table><tr><td>default</td><td>"/root/.twister2/repository/twister2-core/"</td><tr><td>description</td><td></td></table>

**twister2.directory.sandbox.java.home**
<table><tr><td>default</td><td>"${JAVA_HOME}"</td><tr><td>description</td><td>location of java - pick it up from shell environment</td></table>

**#twister2.mesos.master.uri**
<table><tr><td>default</td><td>"149.165.150.81:5050"     #######don't forget to uncomment when pushing####################</td><tr><td>description</td><td>The URI of Mesos Master</td></table>

**twister2.mesos.framework.name**
<table><tr><td>default</td><td>"Twister2 framework"</td><tr><td>description</td><td>mesos framework name</td></table>

**twister2.mesos.master.uri**
<table><tr><td>default</td><td>"zk://localhost:2181/mesos"</td><tr><td>description</td><td></td></table>

**twister2.mesos.framework.staging.timeout.ms**
<table><tr><td>default</td><td>2000</td><tr><td>description</td><td>The maximum time in milliseconds waiting for MesosFramework got registered with Mesos Master</td></table>

**twister2.mesos.scheduler.driver.stop.timeout.ms**
<table><tr><td>default</td><td>5000</td><tr><td>description</td><td>The maximum time in milliseconds waiting for Mesos Scheduler Driver to complete stop()</td></table>

**twister2.mesos.native.library.path**
<table><tr><td>default</td><td>"/usr/lib/mesos/0.28.1/lib/"</td><tr><td>description</td><td>the path to load native mesos library</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><tr><td>description</td><td>the core package uri</td></table>

**twister2.mesos.overlay.network.name**
<table><tr><td>default</td><td>"mesos-overlay"</td><tr><td>description</td><td></td></table>

**twister2.docker.image.name**
<table><tr><td>default</td><td>"gurhangunduz/twister2-mesos:docker-mpi"</td><tr><td>description</td><td></td></table>

**twister2.system.job.uri**
<table><tr><td>default</td><td>"http://localhost:8082/twister2/mesos/twister2-job.tar.gz"</td><tr><td>description</td><td>the job package uri for mesos agent to fetch.<br/>For fetching http server must be running on mesos master</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.mesos.MesosLauncher"</td><tr><td>description</td><td>launcher class for mesos submission</td></table>

**twister2.job.worker.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.examples.internal.comms.BroadcastCommunication"</td><tr><td>description</td><td>container class to run in workers</td></table>

**twister2.class.mesos.worker**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorker"</td><tr><td>description</td><td>the Mesos worker class</td></table>

#### ZooKeeper related config parameters
**twister2.zookeeper.server.addresses**
<table><tr><td>default</td><td>"localhost:2181"</td><tr><td>options</td><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td><tr><td>description</td><td></td></table>

**#twister2.zookeeper.root.node.path**
<table><tr><td>default</td><td>"/twister2"</td><tr><td>description</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td></table>

**twister2.uploader.directory**
<table><tr><td>default</td><td>"/var/www/html/twister2/mesos/"</td><tr><td>description</td><td>the directory where the file will be uploaded, make sure the user has the necessary permissions<br/>to upload the file here.</td></table>

**#twister2.uploader.directory.repository**
<table><tr><td>default</td><td>"/var/www/html/twister2/mesos/"</td><tr><td>description</td><td></td></table>

**twister2.uploader.scp.command.options**
<table><tr><td>default</td><td>"--chmod=+rwx"</td><tr><td>description</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td></table>

**twister2.uploader.scp.command.connection**
<table><tr><td>default</td><td>"root@149.165.150.81"</td><tr><td>description</td><td>The scp connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.uploader.ssh.command.options**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td></table>

**twister2.uploader.ssh.command.connection**
<table><tr><td>default</td><td>"root@149.165.150.81"</td><tr><td>description</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><tr><td>options</td><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"<br/>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uploader class</td></table>

**twister2.uploader.download.method**
<table><tr><td>default</td><td>"HTTP"</td><tr><td>description</td><td>this is the method that workers use to download the core and job packages<br/>it could be  HTTP, HDFS, ..</td></table>

**twister2.HTTP.fetch.uri**
<table><tr><td>default</td><td>"http://149.165.150.81:8082"</td><tr><td>description</td><td>HTTP fetch uri</td></table>

#### Client configuration parameters for submission of twister2 jobs
**twister2.resource.scheduler.mesos.cluster**
<table><tr><td>default</td><td>"example"</td><tr><td>description</td><td>cluster name mesos scheduler runs in</td></table>

**twister2.resource.scheduler.mesos.role**
<table><tr><td>default</td><td>"www-data"</td><tr><td>description</td><td>role in cluster</td></table>

**twister2.resource.scheduler.mesos.env**
<table><tr><td>default</td><td>"devel"</td><tr><td>description</td><td>environment name</td></table>

**twister2.job.name**
<table><tr><td>default</td><td>"basic-mesos"</td><tr><td>description</td><td>mesos job name</td></table>

**  #  workersPerPod**
<table><tr><td>default</td><td>2 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td><tr><td>description</td><td>A Twister2 job can have multiple sets of compute resources<br/>instances shows the number of compute resources to be started with this specification<br/>workersPerPod shows the number of workers on each pod in Kubernetes.<br/>   May be omitted in other clusters. default value is 1.</td></table>

**    instances**
<table><tr><td>default</td><td>4 # number of compute resource instances with this specification</td><tr><td>options</td><td>2 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td><tr><td>description</td><td></td></table>

**twister2.worker.additional.ports**
<table><tr><td>default</td><td>["port1", "port2", "port3"]</td><tr><td>description</td><td>by default each worker has one port<br/>additional ports can be requested for all workers in a job<br/>please provide the requested port names as a list</td></table>

**twister2.job.driver.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.examples.internal.rsched.DriverExample"</td><tr><td>description</td><td>driver class to run</td></table>

**nfs.server.address**
<table><tr><td>default</td><td>"149.165.150.81"</td><tr><td>description</td><td>nfs server address</td></table>

**nfs.server.path**
<table><tr><td>default</td><td>"/nfs/shared-mesos/twister2"</td><tr><td>description</td><td>nfs server path</td></table>

**twister2.worker_port**
<table><tr><td>default</td><td>"31000"</td><tr><td>description</td><td>worker port</td></table>

**twister2.desired_nodes**
<table><tr><td>default</td><td>"all"</td><tr><td>description</td><td>desired nodes</td></table>

**twister2.use_docker_container**
<table><tr><td>default</td><td>"true"</td><tr><td>description</td><td></td></table>

**rack.labey.key**
<table><tr><td>default</td><td>rack</td><tr><td>description</td><td>rack label key for Mesos nodes in a cluster<br/>each rack should have a unique label<br/>all nodes in a rack should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td></table>

**datacenter.labey.key**
<table><tr><td>default</td><td>datacenter</td><tr><td>description</td><td>data center label key<br/>each data center should have a unique label<br/>all nodes in a data center should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td></table>

**  - echo**
<table><tr><td>default</td><td>['blue-rack', 'green-rack']</td><tr><td>description</td><td>Data center list with rack names</td></table>

**  - blue-rack**
<table><tr><td>default</td><td>['10.0.0.40', '10.0.0.41', '10.0.0.42', '10.0.0.43', '10.0.0.44', ]</td><tr><td>description</td><td>Rack list with node IPs in them</td></table>

### Mesos Core Configurations



#### Logging related settings for Twister2 workers
**twister2.logging.level**
<table><tr><td>default</td><td>"INFO"</td><tr><td>description</td><td>default value is INFO</td></table>

**persistent.logging.requested**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>Do workers request persistent logging? it could be true or false<br/>default value is false</td></table>

**twister2.logging.redirect.sysouterr**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>whether System.out and System.err should be redircted to log files<br/>When System.out and System.err are redirected to log file,<br/>All messages are only saved in log files. Only a few intial messages are shown on Dashboard.<br/>Otherwise, Dashboard has the complete messages,<br/>log files has the log messages except System.out and System.err.</td></table>

**twister2.logging.max.file.size.mb**
<table><tr><td>default</td><td>100</td><tr><td>description</td><td>The maximum log file size in MB</td></table>

**twister2.logging.maximum.files**
<table><tr><td>default</td><td>5</td><tr><td>description</td><td>The maximum number of log files for each worker</td></table>

#### Twister2 Job Master related settings
**twister2.job.master.runs.in.client**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster<br/>by default, it is true<br/>when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td></table>

**twister2.job.master.assigns.worker.ids**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when regitering with the job master</td></table>

**twister2.worker.ping.interval**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td>ping message intervals from workers to the job master in milliseconds<br/>default value is 10seconds = 10000</td></table>

**#twister2.job.master.port**
<table><tr><td>default</td><td>2023</td><tr><td>description</td><td>twister2 job master port number<br/>default value is 11111</td></table>

**twister2.worker.to.job.master.response.wait.duration**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td>worker to job master response wait time in milliseconds<br/>this is for messages that wait for a response from the job master<br/>default value is 10seconds = 10000</td></table>

**twister2.job.master.volatile.volume.size**
<table><tr><td>default</td><td>1.0</td><tr><td>description</td><td>twister2 job master volatile volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, volatile volume is not setup for job master</td></table>

**twister2.job.master.persistent.volume.size**
<table><tr><td>default</td><td>1.0</td><tr><td>description</td><td>twister2 job master persistent volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, persistent volume is not setup for job master</td></table>

**twister2.job.master.cpu**
<table><tr><td>default</td><td>0.2</td><tr><td>description</td><td>twister2 job master cpu request<br/>default value is 0.2 percentage</td></table>

**twister2.job.master.ram**
<table><tr><td>default</td><td>1000</td><tr><td>description</td><td>twister2 job master RAM request in MB<br/>default value is 0.2 percentage</td></table>

**twister2.job.master.ip**
<table><tr><td>default</td><td>"149.165.150.81"</td><tr><td>description</td><td></td></table>

#### WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></table>

#### Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://localhost:8080"</td><tr><td>description</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></table>

### Mesos Task Configurations



## Nomad configurations
### Nomad Checkpoint Configurations



### Nomad Data Configurations



### Nomad Network Configurations



**twister2.network.channel.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.comms.tcp.TWSTCPChannel"</td><tr><td>description</td><td></td></table>

### Nomad Resource Configurations



**twister2.resource.scheduler.mpi.working.directory**
<table><tr><td>default</td><td>"${HOME}/.twister2/jobs"</td><tr><td>description</td><td>working directory</td></table>

**twister2.job.package.url**
<table><tr><td>default</td><td>"http://localhost:8082/twister2/mesos/twister2-job.tar.gz"</td><tr><td>description</td><td></td></table>

**twister2.core.package.url**
<table><tr><td>default</td><td>"http://localhost:8082/twister2/mesos/twister2-core-0.2.2.tar.gz"</td><tr><td>description</td><td></td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.nomad.NomadLauncher"</td><tr><td>description</td><td>the launcher class</td></table>

**#twister2.nomad.scheduler.uri**
<table><tr><td>default</td><td>"http://149.165.150.81:4646"</td><tr><td>description</td><td>The URI of Nomad API</td></table>

**twister2.nomad.scheduler.uri**
<table><tr><td>default</td><td>"http://127.0.0.1:4646"</td><tr><td>description</td><td></td></table>

**twister2.nomad.core.freq.mapping**
<table><tr><td>default</td><td>2000</td><tr><td>description</td><td>The nomad schedules cpu resources in terms of clock frequency (e.g. MHz), while Heron topologies<br/>specify cpu requests in term of cores.  This config maps core to clock freqency.</td></table>

**twister2.filesystem.shared**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>weather we are in a shared file system, if that is the case, each worker will not download the<br/>core package and job package, otherwise they will download those packages</td></table>

**twister2.nomad.shell.script**
<table><tr><td>default</td><td>"nomad.sh"</td><tr><td>description</td><td>name of the script</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.3.0.tar.gz"</td><tr><td>description</td><td>path to the system core package</td></table>

**twister2.zookeeper.server.addresses**
<table><tr><td>default</td><td>"localhost:2181"</td><tr><td>options</td><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td><tr><td>description</td><td></td></table>

**twister2.uploader.directory**
<table><tr><td>default</td><td>"/tmp/repository"</td><tr><td>description</td><td>the directory where the file will be uploaded, make sure the user has the necessary permissions<br/>to upload the file here.<br/>if you want to run it on a local machine use this value</td></table>

**#twister2.uploader.directory**
<table><tr><td>default</td><td>"/var/www/html/twister2/mesos/"</td><tr><td>description</td><td>if you want to use http server on echo</td></table>

**twister2.uploader.directory.repository**
<table><tr><td>default</td><td>"/var/www/html/twister2/mesos/"</td><tr><td>description</td><td></td></table>

**twister2.uploader.scp.command.options**
<table><tr><td>default</td><td>"--chmod=+rwx"</td><tr><td>description</td><td>This is the scp command options that will be used by the uploader, this can be used to<br/>specify custom options such as the location of ssh keys.</td></table>

**twister2.uploader.scp.command.connection**
<table><tr><td>default</td><td>"root@localhost"</td><tr><td>description</td><td>The scp connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.uploader.ssh.command.options**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>The ssh command options that will be used when connecting to the uploading host to execute<br/>command such as delete files, make directories.</td></table>

**twister2.uploader.ssh.command.connection**
<table><tr><td>default</td><td>"root@localhost"</td><tr><td>description</td><td>The ssh connection string sets the remote user name and host used by the uploader.</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>options</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><tr><td>description</td><td>file system uploader to be used</td></table>

**twister2.uploader.download.method**
<table><tr><td>default</td><td>"LOCAL"</td><tr><td>description</td><td>this is the method that workers use to download the core and job packages<br/>it could be  LOCAL,  HTTP, HDFS, ..</td></table>

#### client related configurations for job submit
**nfs.server.address**
<table><tr><td>default</td><td>"localhost"</td><tr><td>description</td><td>nfs server address</td></table>

**nfs.server.path**
<table><tr><td>default</td><td>"/nfs/shared/twister2"</td><tr><td>description</td><td>nfs server path</td></table>

**rack.labey.key**
<table><tr><td>default</td><td>rack</td><tr><td>description</td><td>rack label key for Mesos nodes in a cluster<br/>each rack should have a unique label<br/>all nodes in a rack should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td></table>

**datacenter.labey.key**
<table><tr><td>default</td><td>datacenter</td><tr><td>description</td><td>data center label key<br/>each data center should have a unique label<br/>all nodes in a data center should share this label<br/>Twister2 workers can be scheduled by using these label values<br/>Better data locality can be achieved<br/>no default value is specified</td></table>

**  - echo**
<table><tr><td>default</td><td>['blue-rack', 'green-rack']</td><tr><td>description</td><td>Data center list with rack names</td></table>

**  - green-rack**
<table><tr><td>default</td><td>['node11.ip', 'node12.ip', 'node13.ip']</td><tr><td>description</td><td>Rack list with node IPs in them</td></table>

**  #  workersPerPod**
<table><tr><td>default</td><td>2 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td><tr><td>description</td><td>A Twister2 job can have multiple sets of compute resources<br/>instances shows the number of compute resources to be started with this specification<br/>workersPerPod shows the number of workers on each pod in Kubernetes.<br/>   May be omitted in other clusters. default value is 1.</td></table>

**    instances**
<table><tr><td>default</td><td>4 # number of compute resource instances with this specification</td><tr><td>options</td><td>2 # number of workers on each pod in Kubernetes. May be omitted in other clusters.</td><tr><td>description</td><td></td></table>

**twister2.worker.additional.ports**
<table><tr><td>default</td><td>["port1", "port2", "port3"]</td><tr><td>description</td><td>by default each worker has one port<br/>additional ports can be requested for all workers in a job<br/>please provide the requested port names as a list</td></table>

**twister2.worker_port**
<table><tr><td>default</td><td>"31000"</td><tr><td>description</td><td>worker port</td></table>

### Nomad Core Configurations



#### Logging related settings<br/>for Twister2 workers
**twister2.logging.level**
<table><tr><td>default</td><td>"INFO"</td><tr><td>description</td><td>logging level, FINEST, FINER, FINE, CONFIG, INFO, WARNING, SEVERE</td></table>

**persistent.logging.requested**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>Do workers request persistent logging? it could be true or false<br/>default value is false</td></table>

**twister2.logging.redirect.sysouterr**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>whether System.out and System.err should be redirected to log files<br/>When System.out and System.err are redirected to log file,<br/>All messages are only saved in log files. Only a few initial messages are shown on Dashboard.<br/>Otherwise, Dashboard has the complete messages,<br/>log files has the log messages except System.out and System.err.</td></table>

**twister2.logging.max.file.size.mb**
<table><tr><td>default</td><td>100</td><tr><td>description</td><td>The maximum log file size in MB</td></table>

**twister2.logging.maximum.files**
<table><tr><td>default</td><td>5</td><tr><td>description</td><td>The maximum number of log files for each worker</td></table>

**twister2.logging.sandbox.logging**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td></td></table>

#### Twister2 Job Master related settings
**twister2.job.master.runs.in.client**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster<br/>by default, it is true<br/>when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td></table>

**twister2.job.master.assigns.worker.ids**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when registering with the job master</td></table>

**twister2.worker.ping.interval**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td>ping message intervals from workers to the job master in milliseconds<br/>default value is 10seconds = 10000</td></table>

**twister2.job.master.port**
<table><tr><td>default</td><td>11011</td><tr><td>description</td><td>twister2 job master port number<br/>default value is 11111</td></table>

**twister2.worker.to.job.master.response.wait.duration**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td>worker to job master response wait time in milliseconds<br/>this is for messages that wait for a response from the job master<br/>default value is 10seconds = 10000</td></table>

**twister2.job.master.volatile.volume.size**
<table><tr><td>default</td><td>1.0</td><tr><td>description</td><td>twister2 job master volatile volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, volatile volume is not setup for job master</td></table>

**twister2.job.master.persistent.volume.size**
<table><tr><td>default</td><td>1.0</td><tr><td>description</td><td>twister2 job master persistent volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, persistent volume is not setup for job master</td></table>

**twister2.job.master.cpu**
<table><tr><td>default</td><td>0.2</td><tr><td>description</td><td>twister2 job master cpu request<br/>default value is 0.2 percentage</td></table>

**twister2.job.master.ram**
<table><tr><td>default</td><td>1000</td><tr><td>description</td><td>twister2 job master RAM request in MB<br/>default value is 0.2 percentage</td></table>

**twister2.job.master.ip**
<table><tr><td>default</td><td>"localhost"</td><tr><td>description</td><td>the job master ip to be used, this is used only in client based masters</td></table>

#### WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>1000000</td><tr><td>description</td><td>amount of timeout for all workers to join the job<br/>in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>1000000</td><tr><td>description</td><td>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></table>

#### Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://localhost:8080"</td><tr><td>description</td><td>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></table>

### Nomad Task Configurations



