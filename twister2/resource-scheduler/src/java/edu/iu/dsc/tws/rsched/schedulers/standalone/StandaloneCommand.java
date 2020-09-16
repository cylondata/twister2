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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.MPIContext;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class StandaloneCommand extends MPICommand {
  private static final Logger LOG = Logger.getLogger(StandaloneCommand.class.getName());

  public StandaloneCommand(Config cfg, String workingDirectory) {
    super(cfg, workingDirectory);
  }

  @Override
  protected String[] killCommand() {
    return new String[0];
  }

  @Override
  public String[] mpiCommand(String workingDir, JobAPI.Job job) {
    String twister2Home = Paths.get(workingDir, job.getJobId()).toString();
    String confDir = Paths.get(
        workingDir, job.getJobId(), SchedulerContext.clusterType(config))
        .toString();
    String hostfile = Paths.get(confDir, MPIContext.nodesFile(config)).toString();

    String[] mpirunCmd = new String[] {
        mpirunPath(),
        "-np", getNumberOfWorkers(job),
        "--map-by", getMapBy(job),
        "--hostfile", hostfile,
        "-x", "LD_LIBRARY_PATH=" + ldLibraryPath(),
        "-x", "XMX_VALUE=" + getMemory(job) + "m",
        "-x", "XMS_VALUE=" + getMemory(job) + "m",
        "-x", "SUBMITTING_TWISTER2_HOME=" + submittingTwister2Home(),
        "-x", "CLASSPATH=" + getClasspath(config, job),
        "-x", "ILLEGAL_ACCESS_WARN=" + illegalAccessWarn(),
        "-x", "DEBUG=" + getDebug(),
        "-x", "JOB_ID=" + job.getJobId(),
        "-x", "TWISTER2_HOME=" + twister2Home,
        "-x", "CONFIG_DIR=" + twister2Home,
        "-x", "JOB_MASTER_IP=" + config.getStringValue("__job_master_ip__", "ip"),
        "-x", "JOB_MASTER_PORT=" + config.getIntegerValue("__job_master_port__", 0) + "",
        "-x", "RESTORE_JOB= " + CheckpointingContext.startingFromACheckpoint(config)
    };

    List<String> cmdList = new ArrayList<>();
    cmdList.addAll(Arrays.asList(mpirunCmd));

    // add mpi parameters
    String mpiParams = MPIContext.mpiParams(config);
    if (mpiParams != null && !mpiParams.trim().isEmpty()) {
      cmdList.addAll(Arrays.asList(mpiParams.split(" ")));
    }

    // add restart count as the last parameter
    // restart count has to be added as the last parameter
    // since we are updating it in subsequent resubmissions in case of failures
    cmdList.add("-x");
    cmdList.add("RESTART_COUNT=" + 0);

    // add mpi script to run as the last command
    cmdList.add(MPIContext.mpiScriptWithPath(config));

    return cmdList.toArray(new String[]{});
  }

  private String getDebug() {
    if (config.getBooleanValue(SchedulerContext.DEBUG, false)) {
      return "debug";
    } else {
      return "no-debug";
    }
  }

  protected String illegalAccessWarn() {
    //todo remove this once kryo is updated to 5+
    if (getJavaVersion() >= 9) {
      return "suppress_illegal_access_warn";
    } else {
      return "allow_illegal_access_warn";
    }
  }

  /**
   * Temp util method to determine the java version. This will be used to
   * add java9+ flags, to resolve some reflection access warnings.
   * todo remove once Kryo is updated to 5+
   */
  private static int getJavaVersion() {
    String version = System.getProperty("java.version");
    if (version.startsWith("1.")) {
      version = version.substring(2, 3);
    } else {
      int dot = version.indexOf(".");
      if (dot != -1) {
        version = version.substring(0, dot);
      }
    }
    LOG.info("Java version : " + version);
    return Integer.parseInt(version);
  }

  @Override
  protected void updateRestartCount(String[] cmd, int restartCount) {
    cmd[cmd.length - 2] = "RESTART_COUNT=" + restartCount;
  }

}
