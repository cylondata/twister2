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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.exceptions.LauncherException;
import edu.iu.dsc.tws.rsched.exceptions.UploaderException;
import edu.iu.dsc.tws.rsched.interfaces.ILauncher;
import edu.iu.dsc.tws.rsched.interfaces.IUploader;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.uploaders.scp.ScpContext;
import edu.iu.dsc.tws.rsched.utils.FileUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.TarGzipPacker;

/**
 * This is the main class that allocates the resources and starts the processes required
 * <p>
 * These are the steps for submitting a job
 * <p>
 * 1. Figure out the environment from the place where this is executed
 * We will take properties from java system < environment < job
 * 1. Create the job information file and save it
 * 2. Create a job package with jars and job information file to be uploaded to the cluster
 */
public class ResourceAllocator {
  public static final Logger LOG = Logger.getLogger(ResourceAllocator.class.getName());

  private JobAPI.Job updatedJob;
  private Config updatedConfig;

  /**
   * loadConfig from config files and also from envirobnment variables
   *
   * @param cfg the config values in this map will be put into returned Config
   */
  public static Config loadConfig(Map<String, Object> cfg) {
    // first lets read the essential properties from java system properties
    String twister2Home = System.getProperty(SchedulerContext.TWISTER_2_HOME);
    String configDir = System.getProperty(SchedulerContext.CONFIG_DIR);
    String clusterType = System.getProperty(SchedulerContext.CLUSTER_TYPE);
    // lets get the job jar file from system properties or environment
    String jobJar = System.getProperty(SchedulerContext.USER_JOB_JAR_FILE);

    // now lets see weather these are overridden in environment variables
    Map<String, Object> environmentProperties = JobUtils.readCommandLineOpts();

    if (environmentProperties.containsKey(SchedulerContext.TWISTER_2_HOME)) {
      twister2Home = (String) environmentProperties.get(SchedulerContext.CONFIG_DIR);
    }

    if (environmentProperties.containsKey(SchedulerContext.CONFIG_DIR)) {
      configDir = (String) environmentProperties.get(SchedulerContext.CONFIG_DIR);
    }

    if (environmentProperties.containsKey(SchedulerContext.CLUSTER_TYPE)) {
      clusterType = (String) environmentProperties.get(SchedulerContext.CLUSTER_TYPE);
    }

    if (environmentProperties.containsKey(SchedulerContext.USER_JOB_JAR_FILE)) {
      jobJar = (String) environmentProperties.get(SchedulerContext.USER_JOB_JAR_FILE);
    }

    if (configDir == null) {
      configDir = twister2Home + "/conf";
    }

    LOG.log(Level.INFO, String.format("Loading configuration with twister2_home: %s and "
        + "configuration: %s and cluster: %s", twister2Home, configDir, clusterType));
    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);

    // if this is a Kubernetes cluster and Kubernetes upload method is set as client-to-pods
    // do not use a regular uploader
    // Kubernetes client will directly upload the job package to the pods
    String uploaderClass = SchedulerContext.uploaderClass(config);
    String nullUploader = "edu.iu.dsc.tws.rsched.uploaders.NullUploader";
    if (clusterType.equalsIgnoreCase(KubernetesConstants.KUBERNETES_CLUSTER_TYPE)
        && KubernetesContext.uploadMethod(config).equalsIgnoreCase("client-to-pods")
        && !uploaderClass.equalsIgnoreCase(nullUploader)) {

      uploaderClass = nullUploader;
      LOG.info("Since this is a Kubernetes cluster and the upload method is set as client-to-pods,"
          + " uploader class is set to " + uploaderClass);
    }

    return Config.newBuilder().
        putAll(config).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(SchedulerContext.USER_JOB_JAR_FILE, jobJar).
        put(SchedulerContext.UPLOADER_CLASS, uploaderClass).
        putAll(environmentProperties).
        putAll(cfg).
        build();
  }

  /**
   * Create the job files to be uploaded into the cluster
   */
  private String prepareJobFiles(Config config, JobAPI.Job job) {
    // lets first save the job file
    // lets write the job into file, this will be used for job creation
    String tempDirectory = SchedulerContext.jobClientTempDirectory(config) + "/" + job.getJobName();
    try {
      Files.createDirectories(Paths.get(tempDirectory));
    } catch (IOException e) {
      throw new RuntimeException("Failed to create the base temp directory for job", e);
    }

    String jobJarFile = SchedulerContext.userJobJarFile(config);
    if (jobJarFile == null) {
      throw new RuntimeException("Job file cannot be null");
    }

    Path tempDirPath = null;
    try {
      tempDirPath = Files.createTempDirectory(Paths.get(tempDirectory), job.getJobName());
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temp directory: " + tempDirectory, e);
    }

    // temp directory to put archive files
    String tempDirPathString = tempDirPath.toString();

    // copy the core dist package to temp directory
    // do not copy if its a kubernetes cluster
    String clusterType = SchedulerContext.clusterType(config);
    if ("kubernetes".equalsIgnoreCase(clusterType)) {
      LOG.log(Level.INFO, "This is a kubernetes cluster, not moving twister2 core package to temp");

    } else {
      String twister2CorePackage = SchedulerContext.systemPackageUrl(config);
      if (twister2CorePackage == null) {
        throw new RuntimeException("Core package is not specified in the confiuration");
      }
      LOG.log(Level.INFO, String.format("Copy core package: %s to %s",
          twister2CorePackage, tempDirPathString));
      if (!FileUtils.copyFileToDirectory(twister2CorePackage, tempDirPathString)) {
        throw new RuntimeException("Failed to copy the core package");
      }
    }

    // construct an archive file with: job description file, job jar and conf dir
    TarGzipPacker packer = TarGzipPacker.createTarGzipPacker(tempDirPathString, config);
    if (packer == null) {
      throw new RuntimeException("Failed to created the archive file.");
    }

    // first update the job description
    // get file name without directory
    String jobJarFileName = Paths.get(jobJarFile).getFileName().toString();
    JobAPI.JobFormat.Builder format = JobAPI.JobFormat.newBuilder();
//    format.setType(JobAPI.JobFormatType.SHUFFLE);
    format.setJobFile(jobJarFileName);
    updatedJob = JobAPI.Job.newBuilder(job).setJobFormat(format).build();

    // add job description file to the archive
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(job.getJobName());
    boolean added = packer.addFileToArchive(jobDescFileName, updatedJob.toByteArray());
    if (!added) {
      throw new RuntimeException("Failed to add the job description file to the archive: "
          + jobDescFileName);
    }

    // add job jar file to the archive
    added = packer.addFileToArchive(jobJarFile);
    if (!added) {
      throw new RuntimeException("Failed to add the job jar file to the archive: " + jobJarFile);
    }

    // add conf dir to the archive
    String confDir = SchedulerContext.conf(config);
    added = packer.addDirectoryToArchive(confDir);
    if (!added) {
      throw new RuntimeException("Failed to add the conf dir to the archive: " + confDir);
    }

    // close the archive file
    packer.close();
    LOG.log(Level.INFO, "Archive file created: " + packer.getArchiveFileName());

    // add the job description filename, userJobJar and conf directory to the config
    updatedConfig = Config.newBuilder()
        .putAll(config)
        .put(SchedulerContext.USER_JOB_JAR_FILE, jobJarFileName)
        .put(SchedulerContext.TEMPORARY_PACKAGES_PATH, tempDirPathString)
        .build();

    return tempDirPathString;
  }

  /**
   * Submit the job to the cluster
   *
   * @param job the actual job description
   */
  public void submitJob(JobAPI.Job job, Config config) {
    // lets prepare the job files
    String jobDirectory = prepareJobFiles(config, job);

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

    LOG.log(Level.INFO, "Initialize uploader");
    // now upload the content of the package
    uploader.initialize(config);
    // gives the url of the file to be uploaded
    LOG.log(Level.INFO, "Calling uploader to upload the package content");
    URI packageURI = uploader.uploadPackage(jobDirectory);

    // add scp address as a prefix to returned URI: user@ip
    String scpServerAdress = ScpContext.scpConnection(updatedConfig);
    String scpPath = scpServerAdress + ":" + packageURI.toString() + "/";
    LOG.log(Level.INFO, "SCP PATH to copy files from: " + scpPath);

    // now launch the launcher
    // Update the runtime config with the packageURI
    updatedConfig = Config.newBuilder()
        .putAll(updatedConfig)
        .put(SchedulerContext.TWISTER2_PACKAGES_PATH, scpPath)
        .put(SchedulerContext.JOB_PACKAGE_URI, packageURI)
        .build();

    // this is a handler chain based execution in resource allocator. We need to
    // make it more formal as such
    launcher.initialize(updatedConfig);

    RequestedResources requestedResources = buildRequestedResources(updatedJob);
    launcher.launch(requestedResources, updatedJob);
  }

  /**
   * Terminate a job
   *
   * @param jobName the name of the job to terminate
   */
  public void terminateJob(String jobName, Config config) {

    String launcherClass = SchedulerContext.launcherClass(config);
    if (launcherClass == null) {
      throw new RuntimeException("The launcher class must be specified");
    }

    ILauncher launcher;

    // create an instance of launcher
    try {
      launcher = ReflectionUtils.newInstance(launcherClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new LauncherException(
          String.format("Failed to instantiate launcher class '%s'", launcherClass), e);
    }

    // initialize the launcher and terminate the job
    launcher.initialize(config);
    boolean terminated = launcher.terminateJob(jobName);
    if (!terminated) {
      LOG.log(Level.SEVERE, "Could not terminate the job");
    }
  }

  private RequestedResources buildRequestedResources(JobAPI.Job job) {
    JobAPI.JobResources jobResources = job.getJobResources();
    JobAPI.WorkerComputeResource resource =
        jobResources.getResourcesList().get(0).getWorkerComputeResource();
    WorkerComputeResource computeResource =
        new WorkerComputeResource(resource.getCpu(), resource.getRam(), resource.getDisk());

    return new RequestedResources(job.getNumberOfWorkers(), computeResource);
  }
}
