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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
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
  protected List<String> mpiCommand(String workingDirectory, JobAPI.Job job) {
    String twister2Home = Paths.get(workingDirectory, job.getJobName()).toString();
    String configDirectoryName = Paths.get(workingDirectory,
        job.getJobName(), SchedulerContext.clusterType(config)).toString();
    String nodesFileName = MPIContext.nodeFiles(config);

    // lets construct the mpi command to launch
    List<String> mpiCommand = mpiCommand(getScriptPath());
    Map<String, Object> map = mpiCommandArguments(config, job);

    mpiCommand.add(map.get("procs").toString());
    mpiCommand.add(map.get("java_props").toString());
    mpiCommand.add(map.get("classpath").toString());
    mpiCommand.add(map.get("container_class").toString());
    mpiCommand.add(job.getJobName());
    mpiCommand.add(twister2Home);
    mpiCommand.add(twister2Home);
    mpiCommand.add(Paths.get(configDirectoryName, nodesFileName).toString());
    mpiCommand.add(MPIContext.mpiRunFile(config));
    mpiCommand.add("-Xmx" + getMemory(job) + "m");
    mpiCommand.add("-Xms" + getMemory(job) + "m");
    mpiCommand.add(config.getIntegerValue("__job_master_port__", 0) + "");
    mpiCommand.add(config.getStringValue("__job_master_ip__", "ip"));

    //making use of PE of -map-by  of MPI
    int cpusPerProc = 1;
    if (job.getComputeResourceCount() > 0) {
      double cpu = job.getComputeResource(0).getCpu();
      cpusPerProc = (int) Math.ceil(cpu);
    }
    mpiCommand.add(MPIContext.mpiMapBy(config, cpusPerProc));

    if (config.getBooleanValue(SchedulerContext.DEBUG, false)) {
      mpiCommand.add("debug");
    } else {
      mpiCommand.add("no-debug");
    }

    //todo remove this once kryo is updated to 5+
    if (getJavaVersion() >= 9) {
      mpiCommand.add("suppress_illegal_access_warn");
    } else {
      mpiCommand.add("allow_illegal_access_warn");
    }
    return mpiCommand;
  }

  private List<String> mpiCommand(String mpiScript) {
    List<String> slurmCmd;
    slurmCmd = new ArrayList<>(Collections.singletonList(mpiScript));
    return slurmCmd;
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
}
