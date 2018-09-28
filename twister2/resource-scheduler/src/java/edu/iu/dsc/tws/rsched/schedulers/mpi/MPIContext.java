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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.config.TokenSub;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

class MPIContext extends SchedulerContext {
  public static final String WORKING_DIRECTORY =
      "twister2.resource.scheduler.mpi.working.directory";

  public static final String SLURM_JOB_ID = "twister2.resource.scheduler.mpi.job.id";

  public static final String SLURM_SHELL_SCRIPT = "twister2.resource.scheduler.mpi.shell.script";

  public static final String PARTITION = "twister2.resource.scheduler.slurm.partition";
  public static final String MODE = "twsiter2.resource.scheduler.mpi.mode";
  public static final String NODES_FILE = "twister2.resource.scheduler.mpi.nodes.file";
  public static final String MPIRUN_FILE = "twister2.resource.scheduler.mpi.mpirun.file";

  public static String workingDirectory(Config config) {
    return TokenSub.substitute(config, config.getStringValue(WORKING_DIRECTORY,
        "${HOME}/.twister2/jobs"), Context.substitutions);
  }

  public static String jobIdFile(Config config) {
    return config.getStringValue(SLURM_JOB_ID, "mpi-job.pid");
  }

  public static String mpiShellScript(Config config) {
    return config.getStringValue(SLURM_SHELL_SCRIPT, "mpi.sh");
  }

  public static String partition(Config cfg) {
    return cfg.getStringValue(PARTITION);
  }

  public static String mpiMode(Config cfg) {
    return cfg.getStringValue(MODE, "node");
  }

  public static String nodeFiles(Config cfg) {
    return cfg.getStringValue(NODES_FILE, "nodes");
  }

  public static String mpiRunFile(Config cfg) {
    return cfg.getStringValue(MPIRUN_FILE, "mpirun");
  }
}
