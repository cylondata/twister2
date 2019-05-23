# Aurora Resource Configuration



**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.2.1.tar.gz"</td><tr><td>description</td><td>the package uri</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraLauncher</td><tr><td>description</td><td>launcher class for aurora submission</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><tr><td>options</td><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"<br/>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uploader class</td></table>

**twister2.job.worker.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.examples.internal.rsched.BasicAuroraContainer"</td><tr><td>description</td><td>container class to run in workers</td></table>

**twister2.class.aurora.worker**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraWorkerStarter"</td><tr><td>description</td><td>the Aurora worker class</td></table>

### ZooKeeper related config parameters
**twister2.zookeeper.server.addresses**
<table><tr><td>default</td><td>"149.165.150.81:2181"</td><tr><td>options</td><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td><tr><td>description</td><td></td></table>

**#twister2.zookeeper.root.node.path**
<table><tr><td>default</td><td>"/twister2"</td><tr><td>description</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td></table>

**twister2.zookeeper.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>if the workers want to wait for all others to join a job, max wait time in ms</td></table>

