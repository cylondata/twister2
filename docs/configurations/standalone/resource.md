# Standalone Resource Configuration



**twister2.resource.scheduler.mpi.working.directory**
<table><tr><td>default</td><td>"${HOME}/.twister2/jobs"</td><tr><td>description</td><td>working directory</td></table>

**twsiter2.resource.scheduler.mpi.mode**
<table><tr><td>default</td><td>"standalone"</td><tr><td>description</td><td>mode of the mpi scheduler</td></table>

**twister2.resource.scheduler.mpi.job.id**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>the job id file</td></table>

**twister2.resource.scheduler.mpi.shell.script**
<table><tr><td>default</td><td>"mpi.sh"</td><tr><td>description</td><td>slurm script to run</td></table>

**twister2.resource.scheduler.slurm.partition**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>slurm partition</td></table>

**twister2.resource.scheduler.mpi.home**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td>the mpirun command location</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.2.1.tar.gz"</td><tr><td>description</td><td>the package uri</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher"</td><tr><td>description</td><td>the launcher class</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uplaoder class</td></table>

**twister2.resource.scheduler.mpi.mpirun.file**
<table><tr><td>default</td><td>"twister2-core/ompi/bin/mpirun"</td><tr><td>description</td><td>mpi run file, this assumes a mpirun that is shipped with the product<br/>change this to just mpirun if you are using a system wide installation of OpenMPI<br/>or complete path of OpenMPI in case you have something custom</td></table>

**twister2.resource.sharedfs**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>Indicates whether bootstrap process needs to be run and distribute job file and core<br/>between MPI nodes. Twister2 assumes job file is accessible to all nodes if this property is set<br/>to true, else it will run the bootstrap process</td></table>

