# Nomad Resource Configuration



**twister2.resource.scheduler.mpi.working.directory**
<table><tr><td>default</td><td>"${HOME}/.twister2/jobs"</td><tr><td>description</td><td>working directory</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.nomad.NomadLauncher"</td><tr><td>description</td><td>the launcher class</td></table>

**twister2.nomad.scheduler.uri**
<table><tr><td>default</td><td>"http://127.0.0.1:4646"</td><tr><td>description</td><td>The URI of Nomad API</td></table>

**twister2.nomad.core.freq.mapping**
<table><tr><td>default</td><td>2000</td><tr><td>description</td><td>The nomad schedules cpu resources in terms of clock frequency (e.g. MHz), while Heron topologies<br/>specify cpu requests in term of cores.  This config maps core to clock freqency.</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>file system uploader to be used</td></table>

**twister2.filesystem.shared**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>weather we are in a shared file system, if that is the case, each worker will not download the<br/>core package and job package, otherwise they will download those packages</td></table>

**twister2.nomad.shell.script**
<table><tr><td>default</td><td>"nomad.sh"</td><tr><td>description</td><td>name of the script</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.2.1.tar.gz"</td><tr><td>description</td><td>path to the system core package</td></table>

