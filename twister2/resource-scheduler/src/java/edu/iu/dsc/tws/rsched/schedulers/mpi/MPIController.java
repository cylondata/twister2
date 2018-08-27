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

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.interfaces.IController;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

public class MPIController implements IController {
  private static final Logger LOG = Logger.getLogger(MPIController.class.getName());

  private final boolean isVerbose;

  MPIController(boolean isVerbose) {
    this.isVerbose = isVerbose;
  }

  private Config config;
  private String workingDirectory;
  private boolean node = true;
  private MPICommand command;

  @Override
  public void initialize(Config mConfig) {
    this.config = Config.transform(mConfig);

    // get the job working directory
    this.workingDirectory = MPIContext.workingDirectory(config);
    LOG.log(Level.INFO, "Working directory: " + workingDirectory);

    // lets set the mode
    this.node = "node".equals(MPIContext.mpiMode(mConfig));

    // lets creaete the command accordingly
    if (node) {
      command = new NodeCommand(mConfig, workingDirectory);
    } else {
      command = new SlurmCommand(mConfig, workingDirectory);
    }
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public boolean start(RequestedResources resourcePlan, JobAPI.Job job) {
    if (resourcePlan == null || resourcePlan.getNumberOfWorkers() == 0) {
      LOG.log(Level.SEVERE, "No container requested. Can't schedule");
      return false;
    }
    long containers = resourcePlan.getNumberOfWorkers();
    LOG.log(Level.INFO, String.format("Launching job in %s scheduler with no of containers = %d",
        MPIContext.clusterType(config), containers));

    String jobDirectory = Paths.get(this.workingDirectory, job.getJobName()).toString();
    boolean jobCreated = createJob(this.workingDirectory, jobDirectory, resourcePlan, job);

    if (!jobCreated) {
      LOG.log(Level.SEVERE, "Failed to create job");
    } else {
      LOG.log(Level.FINE, "Job created successfully");
    }
    return jobCreated;
  }

  @Override
  public boolean kill(JobAPI.Job job) {
    String[] killCommand = command.killCommand();

    StringBuilder stderr = new StringBuilder();
    runProcess(workingDirectory, killCommand, stderr);

    if (!stderr.toString().equals("")) {
      LOG.log(Level.SEVERE, "Failed to kill the job");
    }
    return false;
  }

  /**
   * Create a slurm job. Use the slurm scheduler's sbatch command to submit the job.
   * sbatch allocates the nodes and runs the script specified by slurmScript.
   * This script runs the twister2 executor on each of the nodes allocated.
   *
   * @param jobWorkingDirectory working directory
   * @return true if the job creation is successful
   */
  public boolean createJob(String jobWorkingDirectory, String twister2Home,
                           RequestedResources resources, JobAPI.Job job) {
    // get the command to run the job on Slurm cluster
    List<String> cmds = command.mpiCommand(jobWorkingDirectory, resources, job);

    // change the empty strings of command args to "", because batch
    // doesn't recognize space as an arguments
    List<String> transformedArgs = new ArrayList<>();
    for (int i = 0; i < cmds.size(); i++) {
      String arg = cmds.get(i);
      if (arg == null || arg.trim().equals("")) {
        transformedArgs.add("\"\"");
      } else {
        transformedArgs.add(arg);
      }
    }
    // add the args to the command
    String[] cmdArray = transformedArgs.toArray(new String[0]);
    LOG.log(Level.FINE, "Executing job [" + jobWorkingDirectory + "]:",
        Arrays.toString(cmdArray));
    StringBuilder stderr = new StringBuilder();
    return runProcess(twister2Home, cmdArray, stderr);
  }

  /**
   * This is for unit testing
   */
  protected boolean runProcess(String jobWorkingDirectory, String[] slurmCmd,
                               StringBuilder stderr) {
    File file = jobWorkingDirectory == null ? null : new File(jobWorkingDirectory);
    return 0 == ProcessUtils.runSyncProcess(false, slurmCmd, stderr, file, true);
  }
}
