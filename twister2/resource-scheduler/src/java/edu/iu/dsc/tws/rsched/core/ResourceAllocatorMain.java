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
package edu.iu.dsc.tws.rsched.core;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.slurmmpi.SlurmMPIContext;
import edu.iu.dsc.tws.rsched.spi.scheduler.ILauncher;
import edu.iu.dsc.tws.rsched.spi.scheduler.LauncherException;
import edu.iu.dsc.tws.rsched.spi.statemanager.IStateManager;
import edu.iu.dsc.tws.rsched.spi.uploaders.IUploader;
import edu.iu.dsc.tws.rsched.spi.uploaders.UploaderException;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

/**
 * This is the main class that allocates the resources and starts the processes required
 *
 * These are the steps for submitting a job
 *
 * 1. Figure out the environment from the place where this is executed
 *    We will take properties from java system < environment < job
 * 1. Create the job information file and save it
 * 2. Create a job package with jars and job information file to be uploaded to the cluster
 *
 */
public class ResourceAllocatorMain {
  public static final Logger LOG = Logger.getLogger(ResourceAllocatorMain.class.getName());

  private Config loadConfig(Map<String, Object> cfg) {
    // first lets read the essential properties from java system properties
    String twister2Home = System.getProperty(SchedulerContext.CONFIG_DIR);
    String configDir = System.getProperty(SchedulerContext.CONFIG_DIR);
    String clusterName = System.getProperty(SchedulerContext.CLUSTER_NAME);

    // now lets see weather these are overridden in environment variables
    Map<String, Object> environmentProperties = JobUtils.readCommandLineOpts();

    if (environmentProperties.containsKey(SchedulerContext.TWISTER_2_HOME)) {
      twister2Home = (String) environmentProperties.get(SchedulerContext.CONFIG_DIR);
    }

    if (environmentProperties.containsKey(SchedulerContext.CONFIG_DIR)) {
      configDir = (String) environmentProperties.get(SchedulerContext.CONFIG_DIR);
    }

    if (environmentProperties.containsKey(SchedulerContext.CLUSTER_NAME)) {
      clusterName = (String) environmentProperties.get(SchedulerContext.CLUSTER_NAME);
    }

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterName);
    return Config.newBuilder().putAll(config).
        put(SlurmMPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SlurmMPIContext.TWISTER2_CLUSTER_NAME, clusterName).
        putAll(environmentProperties).putAll(cfg).build();
  }

  /**
   * Create the job files to be uploaded into the cluster
   */
  private String prepareJobFiles(Config config, JobAPI.Job job) {
    // lets first save the job file
    // lets write the job into file, this will be used for job creation
    String tempDirectory = SchedulerContext.jobClientTempDirectory(config)
        + "/" + job.getJobName();

    // lets create the directory
    File tempDir = new File(tempDirectory);
    if (!tempDir.mkdir()) {
      throw new RuntimeException("Failed to create the temp directory");
    }
    // we are going to delete this on exit
    tempDir.deleteOnExit();

    String jobFile = tempDirectory + "/" + "temp.job";
    boolean write = JobUtils.writeJobFile(job, jobFile);
    if (!write) {
      throw new RuntimeException("Failed to write the job file");
    }

    // copy the job files


    return null;
  }

  /**
   * Submit the job to the cluster
   *
   * @param job the actual job
   */
  public void   submitJob(JobAPI.Job job) {
    // first lets load the configurations
    Config config = loadConfig(new HashMap<>());

    // lets prepare the job files
    String jobFile = prepareJobFiles(config, job);

    String statemgrClass = SchedulerContext.stateManagerClass(config);
    if (statemgrClass == null) {
      throw new RuntimeException("The state manager class must be spcified");
    }

    String launcherClass = SchedulerContext.launcherClass(config);
    if (launcherClass == null) {
      throw new RuntimeException("The launcher class must be specified");
    }

    String uploaderClass = SchedulerContext.uploaderClass(config);
    if (uploaderClass == null) {
      throw new RuntimeException("The uploader class must be specified");
    }

    ILauncher launcher;
    IUploader uploader;
    IStateManager statemgr;

    // create an instance of state manager
    try {
      statemgr = ReflectionUtils.newInstance(statemgrClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new JobSubmissionException(
          String.format("Failed to instantiate state manager class '%s'", statemgrClass), e);
    }

    // create an instance of launcher
    try {
      launcher = ReflectionUtils.newInstance(launcherClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new LauncherException(
          String.format("Failed to instantiate launcher class '%s'", launcherClass), e);
    }

    // create an instance of uploader
    try {
      uploader = ReflectionUtils.newInstance(uploaderClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new UploaderException(
          String.format("Failed to instantiate uploader class '%s'", uploaderClass), e);
    }

    // initialize the state manager
    statemgr.initialize(config);

    // now upload the content of the package
    uploader.initialize(config);
    // gives the url of the file to be uploaded
    URI packageURI = uploader.uploadPackage();

    // now launch the launcher
    // Update the runtime config with the packageURI
//    Config runtimeAll = Config.newBuilder()
//        .putAll()
//        .put(SchedulerContext.JOB_PACKAGE_URI, packageURI)
//        .build();
//
//    launcher.initialize(config);
//    launcher.launch();
  }
}
