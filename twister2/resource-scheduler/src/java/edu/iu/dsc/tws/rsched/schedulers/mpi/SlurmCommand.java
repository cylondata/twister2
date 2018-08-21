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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

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
  protected List<String> mpiCommand(String workingDirectory,
                                    RequestedResources resourcePlan, JobAPI.Job job) {
    String twister2Home = Paths.get(workingDirectory, job.getJobName()).toString();
    String configDirectoryName = Paths.get(workingDirectory,
        job.getJobName(), SchedulerContext.clusterType(config)).toString();

    // lets construct the mpi command to launch
    List<String> mpiCommand = mpiCommand(getScriptPath(), 1);
    Map<String, Object> map = mpiCommandArguments(config, resourcePlan, job);

    mpiCommand.add(map.get("procs").toString());
    mpiCommand.add(map.get("java_props").toString());
    mpiCommand.add(map.get("classpath").toString());
    mpiCommand.add(map.get("container_class").toString());
    mpiCommand.add(job.getJobName());
    mpiCommand.add(twister2Home);
    mpiCommand.add(twister2Home);

    return mpiCommand;
  }

  protected String getJobIdFilePath() {
    return new File(workingDirectory, MPIContext.jobIdFile(config)).getPath();
  }

  /**
   * Construct the SLURM Command
   * @param slurmScript slurm script name
   * @param containers number of containers
   * @return list with the command
   */
  private List<String> mpiCommand(String slurmScript,
                                  long containers) {
    String nTasks = String.format("--ntasks=%d", containers);
    List<String> slurmCmd;
    slurmCmd = new ArrayList<>(Arrays.asList("sbatch", "-N",
        Long.toString(containers), nTasks, slurmScript));
    return slurmCmd;
  }
}
