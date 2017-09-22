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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.rsched.spi.scheduler.IController;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

public class SlurmMPIController implements IController {
  private static final Logger LOG = Logger.getLogger(SlurmMPIController.class.getName());

  private final boolean isVerbose;

  SlurmMPIController(boolean isVerbose) {
    this.isVerbose = isVerbose;
  }

  private Config config;
  private String workingDirectory;

  @Override
  public void initialize(Config mConfig) {
    this.config = Config.toClusterMode(mConfig);

    // get the job working directory
    this.workingDirectory = SlurmMPIContext.workingDirectory(config);
    LOG.log(Level.INFO, "Working directory: " + workingDirectory);
  }

  @Override
  public void close() {
    // Nothing to do here
  }

  @Override
  public boolean start(ResourcePlan resourcePlan) {
    if (resourcePlan == null || resourcePlan.getContainers().isEmpty()) {
      LOG.log(Level.SEVERE, "No container requested. Can't schedule");
      return false;
    }
    long containers = resourcePlan.noOfContainers();
    LOG.log(Level.INFO, "Launching job in Slurm scheduler with no of containers = "
        + containers);

    boolean jobCreated = createJob(getTwister2SlurmPath(), SlurmMPIContext.mpiExecFile(config),
        getExecutorCommand(),
        this.workingDirectory, containers);
    if (!jobCreated) {
      LOG.log(Level.SEVERE, "Failed to create job");
    } else {
      LOG.log(Level.FINE, "Job created successfully");
    }
    return jobCreated;
  }

  @Override
  public boolean kill() {
    // get the slurm id
    String file = getJobIdFilePath();
    return killJob(file);
  }


  protected String getJobIdFilePath() {
    return new File(workingDirectory, SlurmMPIContext.jobIdFile(config)).getPath();
  }

  protected String getTwister2SlurmPath() {
    return new File(SlurmMPIContext.conf(config),
        SlurmMPIContext.slurmShellScript(config)).getPath();
  }

  protected String[] getExecutorCommand() {
    String[] executorCmd = mpiCommandArguments(this.config);

    LOG.log(Level.FINE, "Executor command line: ", Arrays.toString(executorCmd));
    return executorCmd;
  }

  /**
   * Create a slurm job. Use the slurm scheduler's sbatch command to submit the job.
   * sbatch allocates the nodes and runs the script specified by slurmScript.
   * This script runs the twister2 executor on each of the nodes allocated.
   *
   * @param slurmScript slurm bash script to execute
   * @param commandArgs arguments to the twister2 executor
   * @param jobWorkingDirectory working directory
   * @param containers number of containers required to run the job
   * @param partition the queue to submit the job
   * @return true if the job creation is successful
   */
  public boolean createJob(String slurmScript, String mpiExec,
                           String[] commandArgs, String jobWorkingDirectory,
                           long containers, String partition) {
    // get the command to run the job on Slurm cluster
    List<String> slurmCmd = slurmCommand(slurmScript, mpiExec, containers, partition);

    // change the empty strings of command args to "", because batch
    // doesn't recognize space as an arguments
    List<String> transformedArgs = new ArrayList<>();
    for (int i = 0; i < commandArgs.length; i++) {
      String arg = commandArgs[i];
      if (arg == null || arg.trim().equals("")) {
        transformedArgs.add("\"\"");
      } else {
        transformedArgs.add(arg);
      }
    }

    // add the args to the command
    slurmCmd.addAll(transformedArgs);
    String[] slurmCmdArray = slurmCmd.toArray(new String[0]);
    LOG.log(Level.INFO, "Executing job [" + jobWorkingDirectory + "]:",
        Arrays.toString(slurmCmdArray));
    StringBuilder stderr = new StringBuilder();
    return runProcess(jobWorkingDirectory, slurmCmdArray, stderr);
  }

  /**
   * Construct the SLURM Command
   * @param slurmScript slurm script name
   * @param mpiExec twister2 executable name
   * @param containers number of containers
   * @param partition the partition to submit the job
   * @return list with the command
   */
  private List<String> slurmCommand(String slurmScript, String mpiExec,
                                    long containers, String partition) {
    String nTasks = String.format("--ntasks=%d", containers);
    List<String> slurmCmd;
    if (partition != null) {
      slurmCmd = new ArrayList<>(Arrays.asList("sbatch", "-N",
          Long.toString(containers), nTasks, "-p", partition, slurmScript, mpiExec));
    } else {
      slurmCmd = new ArrayList<>(Arrays.asList("sbatch", "-N",
          Long.toString(containers), nTasks, slurmScript, mpiExec));
    }
    return slurmCmd;
  }

  /**
   * Create a slurm job. Use the slurm schedule'r sbatch command to submit the job.
   * sbatch allocates the nodes and runs the script specified by slurmScript.
   * This script runs the twister2 executor on each of the nodes allocated.
   *
   * @param slurmScript slurm bash script to execute
   * @param commandArgs arguments to the twister2 executor
   * @param jobWorkingDirectory working directory
   * @param containers number of containers required to run the job
   * @return true if the job creation is successful
   */
  public boolean createJob(String slurmScript, String mpiExec, String[] commandArgs,
                           String jobWorkingDirectory,
                           long containers) {
    return createJob(slurmScript, mpiExec, commandArgs,
        jobWorkingDirectory, containers, null);
  }

  /**
   * This is for unit testing
   */
  protected boolean runProcess(String jobWorkingDirectory, String[] slurmCmd,
                               StringBuilder stderr) {
    File file = jobWorkingDirectory == null ? null : new File(jobWorkingDirectory);
    return 0 == ProcessUtils.runSyncProcess(false, slurmCmd, stderr, file);
  }

  /**
   * Cancel the Slurm job by reading the jobid from the jobIdFile. Uses scancel
   * command to cancel the job. The file contains a single line with the job id.
   * This file is written by the slurm job script after the job is allocated.
   * @param jobIdFile the jobId file
   * @return true if the job is cancelled successfully
   */
  public boolean killJob(String jobIdFile) {
    List<String> jobIdFileContent = readFromFile(jobIdFile);
    if (jobIdFileContent.size() > 0) {
      String[] slurmCmd = new String[]{"scancel", jobIdFileContent.get(0)};
      return runProcess(null, slurmCmd, new StringBuilder());
    } else {
      LOG.log(Level.SEVERE, "Failed to read the Slurm Job id from file: {0}", jobIdFile);
      return false;
    }
  }

  /**
   * Read all the data from a text file line by line
   * For now lets keep this util function here. We need to move it to a util location
   * @param filename name of the file
   * @return string list containing the lines of the file, if failed to read, return an empty list
   */
  protected List<String> readFromFile(String filename) {
    Path path = new File(filename).toPath();
    List<String> result = new ArrayList<>();
    try {
      List<String> tempResult = Files.readAllLines(path);
      if (tempResult != null) {
        result.addAll(tempResult);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read from file. ", e);
    }
    return result;
  }

  protected String[] mpiCommandArguments(Config cfg) {
    List<String> commands = new ArrayList<>();

    return null;
  }
}
