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
package edu.iu.dsc.tws.rsched.schedulers.standalone;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.interfaces.IController;
import edu.iu.dsc.tws.rsched.interfaces.ILauncher;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.ResourceSchedulerUtils;

public class StandaloneLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(StandaloneController.class.getName());

  private Config config;

  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean terminateJob(String jobName) {
    LOG.log(Level.INFO, "Terminating job for cluster: ",
        StandaloneContext.clusterType(config));

    // get the job working directory
    String jobWorkingDirectory = StandaloneContext.workingDirectory(config);
    Config newConfig = Config.newBuilder().putAll(config).put(
        SchedulerContext.WORKING_DIRECTORY, jobWorkingDirectory).build();
    // now start the controller, which will get the resources from
    // slurm and start the job
    IController controller = new StandaloneController(true);
    controller.initialize(newConfig);

    jobWorkingDirectory = Paths.get(jobWorkingDirectory, jobName).toAbsolutePath().toString();
    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobWorkingDirectory, jobName, config);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

    return controller.kill(job);
  }

  @Override
  public boolean launch(RequestedResources resourcePlan, JobAPI.Job job) {
    LOG.log(Level.INFO, "Launching job for cluster {0}",
        StandaloneContext.clusterType(config));

    // get the job working directory
    String jobWorkingDirectory = StandaloneContext.workingDirectory(config);

    if (StandaloneContext.sharedFileSystem(config)) {
      if (!setupWorkingDirectory(job, jobWorkingDirectory)) {
        throw new RuntimeException("Failed to setup the directory");
      }
    }

    Config newConfig = Config.newBuilder().putAll(config).put(
        SchedulerContext.WORKING_DIRECTORY, jobWorkingDirectory).build();
    // now start the controller, which will get the resources from
    // slurm and start the job
    IController controller = new StandaloneController(true);
    controller.initialize(newConfig);

    // start the Job Master locally
    JobMaster jobMaster = null;
    Thread jmThread = null;
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      try {
        int port = JobMasterContext.jobMasterPort(config);
        String hostAddress = JobMasterContext.jobMasterIP(config);
        if (hostAddress == null) {
          hostAddress = InetAddress.getLocalHost().getHostAddress();
        }
        LOG.log(Level.INFO, String.format("Starting the job manager: %s:%d", hostAddress, port));
        jobMaster =
            new JobMaster(config, hostAddress,
                new StandaloneTerminator(), job.getJobName(),
                port,  job.getNumberOfWorkers());
        jobMaster.addShutdownHook();
        jmThread = jobMaster.startJobMasterThreaded();
      } catch (UnknownHostException e) {
        LOG.log(Level.SEVERE, "Exception when getting local host address: ", e);
        throw new RuntimeException(e);
      }
    }

    boolean start = controller.start(resourcePlan, job);
    // now lets wait on client
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      try {
        if (jmThread != null) {
          jmThread.join();
        }
      } catch (InterruptedException ignore) {
      }
    }

    // now we need to terminate the job
    if (!terminateJob(job.getJobName())) {
      LOG.log(Level.INFO, "Failed to terminate job: " + job.getJobName());
    }

    return start;
  }

  /**
   * setup the working directory mainly it downloads and extracts the heron-core-release
   * and job package to the working directory
   * @return false if setup fails
   */
  private boolean setupWorkingDirectory(JobAPI.Job job, String jobWorkingDirectory) {
    // get the path of core release URI
    String corePackage = StandaloneContext.corePackageFileName(config);

    // Form the job package's URI
    String jobPackageURI = StandaloneContext.jobPackageUri(config).toString();

    // copy the files to the working directory
    return ResourceSchedulerUtils.setupWorkingDirectory(
        job.getJobName(),
        jobWorkingDirectory,
        corePackage,
        jobPackageURI,
        Context.verbose(config));
  }
}
