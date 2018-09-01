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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public abstract class MPICommand {
  private static final Logger LOG = Logger.getLogger(MPICommand.class.getName());

  protected String workingDirectory;

  protected Config config;

  public MPICommand(Config cfg, String workingDirectory) {
    this.workingDirectory = workingDirectory;
    this.config = cfg;
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

  protected String getScriptPath() {
    return new File(MPIContext.conf(config),
        MPIContext.mpiShellScript(config)).getPath();
  }


  protected abstract String[] killCommand();

  protected Map<String, Object> mpiCommandArguments(Config cfg,
                                                    RequestedResources requestedResources,
                                                    JobAPI.Job job) {
    Map<String, Object> commands = new HashMap<>();
    // lets get the configurations
    commands.put("procs", requestedResources.getNumberOfWorkers());

    String jobClassPath = JobUtils.jobClassPath(cfg, job, workingDirectory);
    LOG.log(Level.INFO, "Job class path: " + jobClassPath);
    String systemClassPath = JobUtils.systemClassPath(cfg);
    String classPath = jobClassPath + ":" + systemClassPath;
    commands.put("classpath", classPath);
    commands.put("java_props", "");
    commands.put("container_class", job.getWorkerClassName());

    return commands;
  }

  protected abstract List<String> mpiCommand(String wd,
                                             RequestedResources resourcePlan, JobAPI.Job job);
}
