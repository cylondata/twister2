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

    return new String[]{
        "sbatch", "-N", "1",
        "--ntasks=1",
        "--partition=" + MPIContext.partition(config),
        MPIContext.slurmScriptWithPath(config),
        getNumberOfWorkers(job),
        getClasspath(config, job),
        job.getJobId(),
        twister2Home,
        twister2Home,
        mpirunPath(),
        "-Xmx" + getMemory(job) + "m",
        "-Xms" + getMemory(job) + "m",
        config.getIntegerValue("__job_master_port__", 0) + "",
        config.getStringValue("__job_master_ip__", "ip"),
        Boolean.toString(CheckpointingContext.startingFromACheckpoint(config)),
        "0"
    };
  }

  protected String getJobIdFilePath() {
    return new File(workingDirectory, MPIContext.jobIdFile(config)).getPath();
  }

  @Override
  protected void updateRestartCount(String[] cmd, int restartCount) {
    cmd[cmd.length - 1] = "" + restartCount;
  }
}
