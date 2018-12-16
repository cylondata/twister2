# Standalone Resource Configuration

description

<table><tr><td>property</td><td>twister2.resource.scheduler.mpi.working.directory</td><tr><td>default value</td><td>"${HOME}/.twister2/jobs"</td><tr><td>description</td><td> working directory</td></table>

<table><tr><td>property</td><td>twsiter2.resource.scheduler.mpi.mode</td><tr><td>default value</td><td>"standalone"</td><tr><td>description</td><td> mode of the mpi scheduler</td></table>

<table><tr><td>property</td><td>twister2.resource.scheduler.mpi.job.id</td><tr><td>default value</td><td>""</td><tr><td>description</td><td> the job id file</td></table>

<table><tr><td>property</td><td>twister2.resource.scheduler.mpi.shell.script</td><tr><td>default value</td><td>"mpi.sh"</td><tr><td>description</td><td> slurm script to run</td></table>

<table><tr><td>property</td><td>twister2.resource.scheduler.slurm.partition</td><tr><td>default value</td><td>""</td><tr><td>description</td><td> slurm partition</td></table>

<table><tr><td>property</td><td>twister2.resource.scheduler.mpi.home</td><tr><td>default value</td><td>""</td><tr><td>description</td><td> the mpirun command location</td></table>

<table><tr><td>property</td><td>twister2.system.package.uri</td><tr><td>default value</td><td>"${TWISTER2_DIST}/twister2-core-0.1.0.tar.gz"</td><tr><td>description</td><td> the package uri</td></table>

<table><tr><td>property</td><td>twister2.class.launcher</td><tr><td>default value</td><td>"edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher"</td><tr><td>description</td><td> the launcher class</td></table>

<table><tr><td>property</td><td>twister2.class.uploader</td><tr><td>default value</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td> the uplaoder class</td></table>

<table><tr><td>property</td><td>twister2.resource.scheduler.mpi.mpirun.file</td><tr><td>default value</td><td>"twister2-core/ompi/bin/mpirun"</td><tr><td>description</td><td> mpi run file, this assumes a mpirun that is shipped with the product change this to just mpirun if you are using a system wide installation of OpenMPI or complete path of OpenMPI in case you have something custom</td></table>

