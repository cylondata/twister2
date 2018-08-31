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
package edu.iu.dsc.tws.rsched.utils;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public final class JobUtils {
  private static final Logger LOG = Logger.getLogger(JobUtils.class.getName());

  private JobUtils() {
  }

  /**
   * Write the job file
   */
  public static boolean writeJobFile(JobAPI.Job job, String fileName) {
    // lets write a job file
    byte[] jobBytes = job.toByteArray();
    return FileUtils.writeToFile(fileName, jobBytes, true);
  }

  /**
   * Read the job file
   */
  public static JobAPI.Job readJobFile(Config cfg, String fileName) {
    try {
      byte[] fileBytes = FileUtils.readFromFile(fileName);
      JobAPI.Job.Builder builder = JobAPI.Job.newBuilder();

      return builder.mergeFrom(fileBytes).build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to read the job file: " + fileName);
    }
  }

  public static Map<String, Object> readCommandLineOpts() {
    Map<String, Object> ret = new HashMap<>();
    String commandOptions = System.getenv("TWISTER2_OPTIONS");
    if (commandOptions != null) {
      String[] configs = commandOptions.split(",");
      for (String config : configs) {
        String[] options = config.split(":");
        if (options.length == 2) {
          ret.put(options[0], options[1]);
        }
      }
    }
    return ret;
  }

  public static String jobClassPath(Config cfg, JobAPI.Job job, String wd) {
    StringBuilder classPathBuilder = new StringBuilder();
//    LOG.log(Level.INFO, "Job type: " + job.getJobFormat().getType());
//    if (job.getJobFormat().getType() == JobAPI.JobFormatType.SHUFFLE) {
      // Bundled jar
    classPathBuilder.append(
        Paths.get(wd, job.getJobName(), job.getJobFormat().getJobFile()).toString());
//    }
    return classPathBuilder.toString();
  }

  public static String systemClassPath(Config cfg) {
    String libDirectory = SchedulerContext.libDirectory(cfg);
    String libFile = Paths.get(libDirectory).toString();
    String classPath = "";
    File folder = new File(libFile);
    String libName = folder.getName();
    File[] listOfFiles = folder.listFiles();

    if (listOfFiles != null) {
      for (int i = 0; i < listOfFiles.length; i++) {
        if (listOfFiles[i].isFile()) {
          if (!"".equals(classPath)) {
            classPath += ":" + Paths.get(libDirectory, listOfFiles[i].getName()).toString();
          } else {
            classPath += Paths.get(libDirectory, listOfFiles[i].getName()).toString();
          }
        }
      }
    }
    return classPath;
  }

  /**
   * configs from job object will override the ones in config from files if any
   */

  /**
   * [Deprecated Function]
   * **/
 /* public static Config overrideConfigs(JobAPI.Job job, Config config) {
    Config.Builder builder = Config.newBuilder().putAll(config);
    JobAPI.Config conf = job.getConfig();
    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      builder.put(kv.getKey(), kv.getValue());
    }
    return builder.build();
  }*/

  public static Config overrideConfigs(JobAPI.Job job, Config config) {
    Config.Builder builder = Config.newBuilder().putAll(config);
    JobAPI.Config conf = job.getConfig();
    Map<String, ByteString> configMapSerialized = conf.getConfigByteMapMap();
    for (Map.Entry<String, ByteString> e : configMapSerialized.entrySet()) {
      String key = e.getKey();
      byte[] bytes = e.getValue().toByteArray();
      Object object = new KryoSerializer().deserialize(bytes);
      builder.put(key, object);
    }
    return builder.build();
  }

  public static String getJobDescriptionFilePath(String workingDirectory,
                                                 String jobFileName, Config config) {
    return Paths.get(workingDirectory, jobFileName + ".job").toAbsolutePath().toString();
  }

  public static String getJobDescriptionFilePath(String jobFileName, Config config) {
    String home = Context.twister2Home(config);
    return Paths.get(home, jobFileName + ".job").toAbsolutePath().toString();
  }

  /**
   * write the values from Job object to config object
   * only write the values that are initialized
   * @param job
   * @param config
   * @return
   */
  public static Config updateConfigs(JobAPI.Job job, Config config) {
    Config.Builder builder = Config.newBuilder().putAll(config);

    String jobName = job.getJobName();
    if (jobName != null) {
      builder.put(Context.JOB_NAME, jobName);
    }

    String workerClass = job.getWorkerClassName();
    if (workerClass != null) {
      builder.put(SchedulerContext.WORKER_CLASS, workerClass);
    }

    int workerInstances = job.getNumberOfWorkers();
    if (workerInstances > 0) {
      builder.put(Context.TWISTER2_WORKER_INSTANCES, workerInstances);
    }

    double cpuPerWorker =
        job.getJobResources().getResourcesList().get(0).getWorkerComputeResource().getCpu();
    if (cpuPerWorker > 0) {
      builder.put(Context.TWISTER2_WORKER_CPU, cpuPerWorker);
    }

    int ramPerWorker =
        job.getJobResources().getResourcesList().get(0).getWorkerComputeResource().getRam();
    if (ramPerWorker > 0) {
      builder.put(Context.TWISTER2_WORKER_RAM, ramPerWorker);
    }

    double diskPerWorker =
        job.getJobResources().getResourcesList().get(0).getWorkerComputeResource().getDisk();
    if (diskPerWorker > 0) {
      builder.put(Context.WORKER_VOLATILE_DISK, diskPerWorker);
    }

    return builder.build();
  }

}
