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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.MPIContext;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.master.JobMasterContext;
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

  protected String getNumberOfWorkers(JobAPI.Job job) {
    int numberOfWorkers = job.getNumberOfWorkers();
    if (JobMasterContext.isJobMasterUsed(config)
        && !JobMasterContext.jobMasterRunsInClient(config)) {
      numberOfWorkers++;
    }
    return Integer.toString(numberOfWorkers);
  }

  protected String getClasspath(Config cfg, JobAPI.Job job) {
    String jobClassPath = JobUtils.jobClassPath(cfg, job, workingDirectory);
    LOG.log(Level.FINE, "Job class path: " + jobClassPath);
    String systemClassPath = JobUtils.systemClassPath(cfg);
    String classPath = jobClassPath + ":" + systemClassPath;
    return classPath;
  }

  protected abstract String[] killCommand();

  int getMemory(JobAPI.Job job) {
    int memory = 256;
    int mem = job.getComputeResource(0).getRamMegaBytes();
    if (mem > 0) {
      memory = mem;
    }
    return memory;
  }

  protected abstract String[] mpiCommand(String wd, JobAPI.Job job);

  protected abstract void updateRestartCount(String[] cmd, int restartCount);

  protected String mpirunPath() {
    String mpiRunFile = MPIContext.mpiRunFile(config);
    if ("ompi/bin/mpirun".equals(mpiRunFile)) {
      if (SchedulerContext.copySystemPackage(config)) {
        return "twister2-core" + "/" + mpiRunFile;
      } else {
        return SchedulerContext.twister2Home(config) + "/" + mpiRunFile;
      }
    } else {
      return mpiRunFile;
    }
  }

  protected String ldLibraryPath() {
    String mpiRunFile = MPIContext.mpiRunFile(config);
    String current = System.getenv("LD_LIBRARY_PATH");

    if (mpiRunFile != null && mpiRunFile.endsWith("/bin/mpirun")) {
      if (current == null) {
        return mpiRunFile.replace("/bin/mpirun", "/lib");
      } else {
        return current + ":" + mpiRunFile.replace("/bin/mpirun", "/lib");
      }
    }

    return current == null ? "" : current;
  }

  protected String getMapBy(JobAPI.Job job) {
    //making use of PE of -map-by  of MPI
    int cpusPerProc = 1;
    if (job.getComputeResourceCount() > 0) {
      double cpu = job.getComputeResource(0).getCpu();
      cpusPerProc = (int) Math.ceil(cpu);
    }
    return MPIContext.mpiMapBy(config, cpusPerProc);
  }

  protected String submittingTwister2Home() {
    // we are adding the submitting twister2 home at the end
    if (SchedulerContext.copySystemPackage(config)) {
      return "twister2-core";
    } else {
      return SchedulerContext.twister2Home(config);
    }
  }

}
