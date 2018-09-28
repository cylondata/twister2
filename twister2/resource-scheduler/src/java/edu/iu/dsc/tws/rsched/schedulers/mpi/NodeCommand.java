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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class NodeCommand extends MPICommand {
  private static final Logger LOG = Logger.getLogger(NodeCommand.class.getName());
  public NodeCommand(Config cfg, String workingDirectory) {
    super(cfg, workingDirectory);
  }

  @Override
  protected String[] killCommand() {
    return new String[0];
  }

  @Override
  protected List<String> mpiCommand(String workingDirectory,
                                    RequestedResources resourcePlan, JobAPI.Job job) {
    String twister2Home = Paths.get(workingDirectory, job.getJobName()).toString();
    String configDirectoryName = Paths.get(workingDirectory,
        job.getJobName(), SchedulerContext.clusterType(config)).toString();
    String nodesFileName = MPIContext.nodeFiles(config);

    // lets construct the mpi command to launch
    List<String> mpiCommand = mpiCommand(getScriptPath());
    Map<String, Object> map = mpiCommandArguments(config, resourcePlan, job);

    mpiCommand.add(map.get("procs").toString());
    mpiCommand.add(map.get("java_props").toString());
    mpiCommand.add(map.get("classpath").toString());
    mpiCommand.add(map.get("container_class").toString());
    mpiCommand.add(job.getJobName());
    mpiCommand.add(twister2Home);
    mpiCommand.add(twister2Home);
    mpiCommand.add(Paths.get(configDirectoryName, nodesFileName).toString());
    mpiCommand.add(MPIContext.mpiRunFile(config));
    return mpiCommand;
  }

  private List<String> mpiCommand(String mpiScript) {
    List<String> slurmCmd;
    slurmCmd = new ArrayList<>(Collections.singletonList(mpiScript));
    return slurmCmd;
  }
}
