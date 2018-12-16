# Standalone Resource Configuration

description

<h3>twister2.resource.scheduler.mpi.working.directory</h3><table><tr><td>default</td><td>"${HOME}/.twister2/jobs"</td><tr><td>description</td><td> working directory</td></table>

<h3>twsiter2.resource.scheduler.mpi.mode</h3><table><tr><td>default</td><td>"standalone"</td><tr><td>description</td><td> mode of the mpi scheduler</td></table>

<h3>twister2.resource.scheduler.mpi.job.id</h3><table><tr><td>default</td><td>""</td><tr><td>description</td><td> the job id file</td></table>

<h3>twister2.resource.scheduler.mpi.shell.script</h3><table><tr><td>default</td><td>"mpi.sh"</td><tr><td>description</td><td> slurm script to run</td></table>

<h3>twister2.resource.scheduler.slurm.partition</h3><table><tr><td>default</td><td>""</td><tr><td>description</td><td> slurm partition</td></table>

<h3>twister2.resource.scheduler.mpi.home</h3><table><tr><td>default</td><td>""</td><tr><td>description</td><td> the mpirun command location</td></table>

<h3>twister2.system.package.uri</h3><table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.1.0.tar.gz"</td><tr><td>description</td><td> the package uri</td></table>

<h3>twister2.class.launcher</h3><table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher"</td><tr><td>description</td><td> the launcher class</td></table>

<h3>twister2.class.uploader</h3><table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td> the uplaoder class</td></table>

<h3>twister2.resource.scheduler.mpi.mpirun.file</h3><table><tr><td>default</td><td>"twister2-core/ompi/bin/mpirun"</td><tr><td>description</td><td> mpi run file, this assumes a mpirun that is shipped with the product change this to just mpirun if you are using a system wide installation of OpenMPI or complete path of OpenMPI in case you have something custom</td></table>

