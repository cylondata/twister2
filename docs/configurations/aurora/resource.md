# Aurora Resource Configuration



**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><tr><td>options</td><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"<br/>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td><br/>the package uri<br/>launcher class for aurora submission<br/>the uplaoder class</td></table>

**twister2.job.worker.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.examples.internal.rsched.BasicAuroraContainer"</td><tr><td>description</td><td><br/>container class to run in workers</td></table>

**twister2.class.aurora.worker**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraWorkerStarter"</td><tr><td>description</td><td><br/>the Aurora worker class</td></table>

### <br/>ZooKeeper related config parameters
**#twister2.zookeeper.root.node.path**
<table><tr><td>default</td><td>"/twister2"</td><tr><td>description</td><td><br/>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td></table>

**twister2.zookeeper.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td><br/>if the workers want to wait for all others to join a job, max wait time in ms</td></table>

