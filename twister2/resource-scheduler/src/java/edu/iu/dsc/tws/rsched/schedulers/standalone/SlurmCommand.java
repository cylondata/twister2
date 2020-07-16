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

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.MPIContext;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class SlurmCommand extends MPICommand {
  private static final Logger LOG = Logger.getLogger(SlurmCommand.class.getName());

  private String jobIdFile;

  public SlurmCommand(Config cfg, String workingDirectory) {
    super(cfg, workingDirectory);

    this.jobIdFile = getJobIdFilePath();
  }

  @Override
  protected String[] killCommand() {
    String file = getJobIdFilePath();

    List<String> jobIdFileContent = readFromFile(file);

    if (jobIdFileContent.size() > 0) {
      return new String[]{"scancel", jobIdFileContent.get(0)};
    } else {
      LOG.log(Level.SEVERE, "Failed to read the Slurm Job id from file: {0}", jobIdFile);
      return null;
    }
  }

  @Override
  public String[] mpiCommand(String workingDir, JobAPI.Job job) {
    String twister2Home = Paths.get(workingDir, job.getJobId()).toString();

    List<String> cmd = new ArrayList<>();
    cmd.add("sbatch");
    cmd.add("--partition=" + MPIContext.partition(config));
    String slurmParams = MPIContext.slurmParams(config);
    if (slurmParams != null && !slurmParams.trim().isEmpty()) {
      cmd.addAll(Arrays.asList(slurmParams.split(" ")));
    }

    // add mpirun and its parameters
    cmd.add(mpirunPath());
    String mpiParams = MPIContext.mpiParams(config);
    if (mpiParams != null && !mpiParams.trim().isEmpty()) {
      cmd.addAll(Arrays.asList(mpiParams.split(" ")));
    }

    // add java and jvm parameters
    cmd.add("java");
    cmd.add("-Xmx" + getMemory(job) + "m");
    cmd.add("-Xms" + getMemory(job) + "m");
    cmd.add("-Djava.util.logging.config.file=common/logger.properties");
    cmd.add("-cp");
    cmd.add(getClasspath(config, job));

    // add java class and command line parameters
    cmd.add("edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorkerStarter");
    cmd.add("--job_id");
    cmd.add(job.getJobId());
    cmd.add("--twister2_home");
    cmd.add(twister2Home);
    cmd.add("--config_dir");
    cmd.add(twister2Home);
    cmd.add("--cluster_type");
    cmd.add("slurm");
    cmd.add("--job_master_ip");
    cmd.add(config.getStringValue("__job_master_ip__", "ip"));
    cmd.add("--job_master_port");
    cmd.add(config.getIntegerValue("__job_master_port__", 0) + "");
    cmd.add("--restore_job");
    cmd.add(Boolean.toString(CheckpointingContext.startingFromACheckpoint(config)));
    cmd.add("--restart_count");
    cmd.add("0");

    return cmd.toArray(new String[]{});
  }

  protected String getJobIdFilePath() {
    return new File(workingDirectory, MPIContext.jobIdFile(config)).getPath();
  }

  @Override
  protected void updateRestartCount(String[] cmd, int restartCount) {
    cmd[cmd.length - 1] = "" + restartCount;
  }
}
