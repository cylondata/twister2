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
package edu.iu.dsc.tws.rsched.schedulers.slurmmpi;

import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.rsched.spi.resource.RequestedResources;
import edu.iu.dsc.tws.rsched.spi.scheduler.IController;
import edu.iu.dsc.tws.rsched.spi.scheduler.ILauncher;
import edu.iu.dsc.tws.rsched.utils.ResourceSchedulerUtils;

public class SlurmMPILauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(SlurmMPILauncher.class.getName());

  private Config config;

  private String jobWorkingDirectory;

  @Override
  public void initialize(Config mConfig) {
    this.config = mConfig;

    // get the job working directory
    this.jobWorkingDirectory = SlurmMPIContext.workingDirectory(mConfig);
  }

  @Override
  public void close() {

  }

  @Override
  public boolean launch(RequestedResources resourcePlan) {
    LOG.log(Level.FINE, "Launching job for cluster {0}",
        SlurmMPIContext.clusterName(config));

    // download the core and job packages into the working directory
    // this working directory is a shared directory among the nodes
//    if (!setupWorkingDirectory()) {
//      LOG.log(Level.SEVERE, "Failed to download the core and job packages");
//      return false;
//    }

    // now start the controller, which will get the resources from
    // slurm and start the job
    IController controller = new SlurmMPIController(true);
    controller.initialize(config);
    return controller.start(resourcePlan);
  }

  /**
   * setup the working directory mainly it downloads and extracts the heron-core-release
   * and job package to the working directory
   * @return false if setup fails
   */
  protected boolean setupWorkingDirectory() {
    // get the path of core release URI
    String coreReleasePackageURI = SlurmMPIContext.systemPackageUrl(config);

    // form the target dest core release file name
    String coreReleaseFileDestination = Paths.get(
        jobWorkingDirectory, "twister2-system.tar.gz").toString();

    // Form the job package's URI
    String jobPackageURI = SlurmMPIContext.jobPackageUri(config).toString();

    // form the target job package file name
    String jobPackageDestination = Paths.get(
        jobWorkingDirectory, "job.tar.gz").toString();

    // copy the files to the working directory
    return ResourceSchedulerUtils.setupWorkingDirectory(
        jobWorkingDirectory,
        coreReleasePackageURI,
        coreReleaseFileDestination,
        jobPackageURI,
        jobPackageDestination,
        Context.verbose(config));
  }
}
