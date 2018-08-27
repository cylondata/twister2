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
package edu.iu.dsc.tws.rsched.schedulers.mpi;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.interfaces.IController;
import edu.iu.dsc.tws.rsched.interfaces.ILauncher;
import edu.iu.dsc.tws.rsched.utils.ResourceSchedulerUtils;

public class MPILauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(MPILauncher.class.getName());

  private Config config;

  private String jobWorkingDirectory;

  @Override
  public void initialize(Config mConfig) {
    this.config = mConfig;

    // get the job working directory
    this.jobWorkingDirectory = MPIContext.workingDirectory(mConfig);
  }

  @Override
  public void close() {

  }

  @Override
  public boolean terminateJob(String jobName) {
    // not implemented yet
    return false;
  }

  @Override
  public boolean launch(RequestedResources resourcePlan, JobAPI.Job job) {
    LOG.log(Level.INFO, "Launching job for cluster {0}",
        MPIContext.clusterType(config));

    if (!setupWorkingDirectory(job)) {
      throw new RuntimeException("Failed to setup the directory");
    }

    Config newConfig = Config.newBuilder().putAll(config).put(
        SchedulerContext.WORKING_DIRECTORY, jobWorkingDirectory).build();
    // now start the controller, which will get the resources from
    // slurm and start the job
    IController controller = new MPIController(true);
    controller.initialize(newConfig);
    return controller.start(resourcePlan, job);
  }

  /**
   * setup the working directory mainly it downloads and extracts the heron-core-release
   * and job package to the working directory
   * @return false if setup fails
   */
  protected boolean setupWorkingDirectory(JobAPI.Job job) {
    // get the path of core release URI
    String corePackage = MPIContext.corePackageFileName(config);

    // Form the job package's URI
    String jobPackageURI = MPIContext.jobPackageUri(config).toString();

    // copy the files to the working directory
    return ResourceSchedulerUtils.setupWorkingDirectory(
        job.getJobName(),
        jobWorkingDirectory,
        corePackage,
        jobPackageURI,
        Context.verbose(config));
  }
}
