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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class SlurmMPIContext extends SchedulerContext {
  public static final String WORKING_DIRECTORY =
      "twister2.resource.scheduler.slurm.working.directory";

  public static final String SLURM_JOB_ID = "twister2.resource.scheduler.slurm.job.id";

  public static final String SLURM_SHELL_SCRIPT = "twister2.resource.scheduler.slurm.shell.script";

  public static final String PARTITION = "twister2.resource.scheduler.slurm.partition";
  public static final String MPI_HOME = "twister2.resource.scheduler.mpi.home";

  public static String workingDirectory(Config config) {
    return config.getStringValue(WORKING_DIRECTORY, "${HOME}/.twister2data/jobs/${CLUSTER}/${JOB}");
  }

  public static String jobIdFile(Config config) {
    return config.getStringValue(SLURM_JOB_ID, "slurm-job.pid");
  }

  public static String slurmShellScript(Config config) {
    return config.getStringValue(SLURM_SHELL_SCRIPT, "slurm.sh");
  }

  public static String partition(Config cfg) {
    return cfg.getStringValue(PARTITION);
  }

  public static String mpiExecFile(Config cfg) {
    return cfg.getStringValue(MPI_HOME, "");
  }
}
