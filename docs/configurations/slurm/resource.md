# Slurm Resource Configuration



**twister2.resource.scheduler.mpi.working.directory**
<table><tr><td>default</td><td>"${HOME}/.twister2/jobs"</td><tr><td>description</td><td><br/>working directory</td></table>

**twsiter2.resource.scheduler.mpi.mode**
<table><tr><td>default</td><td>"slurm"</td><tr><td>description</td><td><br/>mode of the mpi scheduler</td></table>

**twister2.resource.scheduler.mpi.job.id**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td><br/>the job id file</td></table>

**twister2.resource.scheduler.mpi.shell.script**
<table><tr><td>default</td><td>"mpi.sh"</td><tr><td>description</td><td><br/>slurm script to run</td></table>

**twister2.resource.scheduler.slurm.partition**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td><br/>slurm partition</td></table>

**twister2.resource.scheduler.mpi.home**
<table><tr><td>default</td><td>""</td><tr><td>description</td><td><br/>the mpirun command location</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.1.0.tar.gz"</td><tr><td>description</td><td><br/>the package uri</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher"</td><tr><td>description</td><td><br/>the launcher class</td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td><br/>the uplaoder class</td></table>

**twister2.resource.scheduler.mpi.mpirun.file**
<table><tr><td>default</td><td>"mpirun"</td><tr><td>description</td><td><br/>mpi run file, this assumes a mpirun that is shipped with the product<br/>change this to just mpirun if you are using a system wide installation of OpenMPI<br/>or complete path of OpenMPI in case you have something custom</td></table>

