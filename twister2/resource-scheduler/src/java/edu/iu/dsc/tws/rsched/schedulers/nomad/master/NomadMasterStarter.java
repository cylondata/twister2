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
package edu.iu.dsc.tws.rsched.schedulers.nomad.master;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.scheduler.IController;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.common.driver.IScalerPerCluster;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.nomad.NomadContext;
import edu.iu.dsc.tws.rsched.schedulers.nomad.NomadController;
import edu.iu.dsc.tws.rsched.schedulers.nomad.NomadTerminator;
import edu.iu.dsc.tws.rsched.utils.ResourceSchedulerUtils;


public final class NomadMasterStarter {
  private static final Logger LOG = Logger.getLogger(NomadMasterStarter.class.getName());

  private JobAPI.Job job;
  private Config config;

  public NomadMasterStarter() {
  }

  public void initialize(JobAPI.Job jb, Config cfg) {
    job = jb;
    config = cfg;
  }

  /**
   * launch the job master
   *
   * @return false if setup fails
   */
  public boolean launch() {
    // get the job working directory
    String jobWorkingDirectory = NomadContext.workingDirectory(config);
    LOG.log(Level.INFO, "job working directory ....." + jobWorkingDirectory);

    if (NomadContext.sharedFileSystem(config)) {
      if (!setupWorkingDirectory(job, jobWorkingDirectory)) {
        throw new RuntimeException("Failed to setup the directory");
      }
    }

    Config newConfig = Config.newBuilder().putAll(config).put(
        SchedulerContext.WORKING_DIRECTORY, jobWorkingDirectory).build();
    // now start the controller, which will get the resources from
    // slurm and start the job
    IController controller = new NomadController(true);
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

        JobMasterAPI.NodeInfo jobMasterNodeInfo = NomadContext.getNodeInfo(config, hostAddress);
        IScalerPerCluster clusterScaler = null;
        JobMasterAPI.JobMasterState initialState = JobMasterAPI.JobMasterState.JM_STARTED;
        NomadTerminator nt = new NomadTerminator();

        jobMaster = new JobMaster(
            config, hostAddress, nt, job, jobMasterNodeInfo, clusterScaler, initialState);
        jobMaster.addShutdownHook(true);
        jmThread = jobMaster.startJobMasterThreaded();
      } catch (UnknownHostException e) {
        LOG.log(Level.SEVERE, "Exception when getting local host address: ", e);
        throw new RuntimeException(e);
      }
    }

    boolean start = controller.start(job);
//     now lets wait on client
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      try {
        if (jmThread != null) {
          jmThread.join();
        }
      } catch (InterruptedException ignore) {
      }
    }

    return start;
  }

  /**
   * setup the working directory mainly it downloads and extracts the heron-core-release
   * and job package to the working directory
   *
   * @return false if setup fails
   */
  private boolean setupWorkingDirectory(JobAPI.Job jb, String jobWorkingDirectory) {
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
        jb.getJobName(),
        jobWorkingDirectory,
        corePackage,
        jobPackageURI,
        Context.verbose(config));
  }
}

