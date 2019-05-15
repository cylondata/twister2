# Mesos Resource Configuration



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
<table><tr><td>default</td><td>"zk://149.165.150.81:2181/mesos"</td><tr><td>description</td><td></td></table>

**twister2.mesos.framework.staging.timeout.ms**
<table><tr><td>default</td><td>2000</td><tr><td>description</td><td>The maximum time in milliseconds waiting for MesosFramework got registered with Mesos Master</td></table>

**twister2.mesos.scheduler.driver.stop.timeout.ms**
<table><tr><td>default</td><td>5000</td><tr><td>description</td><td>The maximum time in milliseconds waiting for Mesos Scheduler Driver to complete stop()</td></table>

**twister2.mesos.native.library.path**
<table><tr><td>default</td><td>"/usr/lib/mesos/0.28.1/lib/"</td><tr><td>description</td><td>the path to load native mesos library</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.2.1.tar.gz"</td><tr><td>description</td><td>the core package uri</td></table>

**twister2.mesos.fetch.uri**
<table><tr><td>default</td><td>"http://149.165.150.81:8082"</td><tr><td>description</td><td>mesos fetch uri</td></table>

**twister2.mesos.overlay.network.name**
<table><tr><td>default</td><td>"mesos-overlay"</td><tr><td>description</td><td></td></table>

**twister2.docker.image.name**
<table><tr><td>default</td><td>"gurhangunduz/twister2-mesos:docker-mpi"</td><tr><td>description</td><td></td></table>

**twister2.system.job.uri**
<table><tr><td>default</td><td>"http://149.165.150.81:8082/twister2/mesos/twister2-job.tar.gz"</td><tr><td>description</td><td>the job package uri for mesos agent to fetch.<br/>For fetching http server must be running on mesos master</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.mesos.MesosLauncher"</td><tr><td>description</td><td>launcher class for mesos submission</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><tr><td>options</td><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"<br/>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uploader class</td></table>

**twister2.job.worker.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.examples.internal.comms.BroadcastCommunication"</td><tr><td>description</td><td>container class to run in workers</td></table>

**twister2.class.mesos.worker**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorker"</td><tr><td>description</td><td>the Mesos worker class</td></table>

### ZooKeeper related config parameters
**twister2.zookeeper.server.addresses**
<table><tr><td>default</td><td>"149.165.150.81:2181"</td><tr><td>options</td><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td><tr><td>description</td><td></td></table>

**#twister2.zookeeper.root.node.path**
<table><tr><td>default</td><td>"/twister2"</td><tr><td>description</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td></table>

