//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.rsched.schedulers.nomad;

import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.scheduler.IController;
import edu.iu.dsc.tws.api.scheduler.ILauncher;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.nomad.master.NomadMasterStarter;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.ResourceSchedulerUtils;

public class NomadLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(NomadController.class.getName());

  private Config config;

  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean terminateJob(String jobID) {
    LOG.log(Level.INFO, "Terminating job for cluster: ",
        NomadContext.clusterType(config));

    // get the job working directory
    String jobWorkingDirectory = NomadContext.workingDirectory(config);
    Config newConfig = Config.newBuilder().putAll(config).put(
        SchedulerContext.WORKING_DIRECTORY, jobWorkingDirectory).build();
    // now start the controller, which will get the resources from
    // slurm and start the job
    IController controller = new NomadController(true);
    controller.initialize(newConfig);

    jobWorkingDirectory = Paths.get(jobWorkingDirectory, jobID).toAbsolutePath().toString();
    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobWorkingDirectory, jobID, config);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

    return controller.kill(job);
  }

  @Override
  public Twister2JobState launch(JobAPI.Job job) {
    LOG.log(Level.INFO, "Launching job for cluster {0}",
        NomadContext.clusterType(config));

    Twister2JobState state = new Twister2JobState(false);

    NomadMasterStarter master = new NomadMasterStarter();
    master.initialize(job, config);
    boolean start = master.launch();
    // now we need to terminate the job
    if (!terminateJob(job.getJobId())) {
      LOG.log(Level.INFO, "Failed to terminate job: " + job.getJobId());
    }
    state.setRequestGranted(start);
    return state;
  }

  /**
   * setup the working directory mainly it downloads and extracts the heron-core-release
   * and job package to the working directory
   *
   * @return false if setup fails
   */
  private boolean setupWorkingDirectory(JobAPI.Job job, String jobWorkingDirectory) {
    // get the path of core release URI
    String corePackage = NomadContext.corePackageFileName(config);
    String jobPackage = NomadContext.jobPackageFileName(config);
    LOG.log(Level.INFO, "Core Package is ......: " + corePackage);
    LOG.log(Level.INFO, "Job Package is ......: " + jobPackage);
    // Form the job package's URI
    String jobPackageURI = NomadContext.jobPackageUri(config).toString();
    LOG.log(Level.INFO, "Job Package URI is ......: " + jobPackageURI);
    // copy the files to the working directory
    return ResourceSchedulerUtils.setupWorkingDirectory(
        job.getJobId(),
        jobWorkingDirectory,
        corePackage,
        jobPackageURI,
        Context.verbose(config));
  }
}
