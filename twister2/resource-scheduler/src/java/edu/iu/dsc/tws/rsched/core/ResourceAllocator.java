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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.scheduler.ILauncher;
import edu.iu.dsc.tws.api.scheduler.IUploader;
import edu.iu.dsc.tws.api.scheduler.LauncherException;
import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import edu.iu.dsc.tws.api.scheduler.UploaderException;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;
import edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader;
import edu.iu.dsc.tws.rsched.uploaders.scp.ScpContext;
import edu.iu.dsc.tws.rsched.utils.FileUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.TarGzipPacker;

/**
 * This is the main class that allocates the resources and starts the processes required
 * <p>
 * These are the steps for submitting a job
 * <p>
 * <ol>
 * <li>Figure out the environment from the place where this is executed</li>
 * <li>We will take properties from java {@literal system < environment < job}</li>
 * <li>Create the job information file and save it</li>
 * <li>Create a job package with jars and job information file to be uploaded to the cluster</li>
 * </ol>
 */
public class ResourceAllocator {
  public static final Logger LOG = Logger.getLogger(ResourceAllocator.class.getName());

  private JobAPI.Job job;
  private Config config;

  public ResourceAllocator(Config config, JobAPI.Job job) {
    this.config = config;
    this.job = job;
  }

  /**
   * loadConfig from config files and also from environment variables
   *
   * @param cfg the config values in this map will be put into returned Config
   */
  public static Config loadConfig(Map<String, Object> cfg) {
    // first lets read the essential properties from java system properties
    String twister2Home = System.getProperty(SchedulerContext.TWISTER_2_HOME);
    String configDir = System.getProperty(SchedulerContext.CONFIG_DIR);
    String clusterType = System.getProperty(SchedulerContext.CLUSTER_TYPE);
    // lets get the job jar file from system properties or environment
    String jobJar = System.getProperty(SchedulerContext.USER_JOB_FILE);
    String jobType = System.getProperty(SchedulerContext.USER_JOB_TYPE);

    Boolean debug = Boolean.valueOf(System.getProperty(SchedulerContext.DEBUG));

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

    if (environmentProperties.containsKey(SchedulerContext.USER_JOB_FILE)) {
      jobJar = (String) environmentProperties.get(SchedulerContext.USER_JOB_FILE);
    }

    if (environmentProperties.containsKey(SchedulerContext.USER_JOB_TYPE)) {
      jobType = (String) environmentProperties.get(SchedulerContext.USER_JOB_TYPE);
    }

    if (configDir == null) {
      configDir = twister2Home + "/conf";
    }

    Config cnfg = ConfigLoader.loadConfig(twister2Home, configDir, clusterType);

    // set log level
    // LoggingHelper.setLogLevel(LoggingContext.loggingLevel(config));

    LOG.log(Level.INFO, String.format("Loaded configuration with twister2_home: %s and "
        + "configuration: %s and cluster: %s", twister2Home, configDir, clusterType));

    return Config.newBuilder().
        putAll(cnfg).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(SchedulerContext.USER_JOB_FILE, jobJar).
        put(SchedulerContext.USER_JOB_TYPE, jobType).
        put(SchedulerContext.DEBUG, debug).
        putAll(environmentProperties).
        putAll(cfg).
        build();
  }

  /**
   * Returns the default configuration
   */
  public static Config getDefaultConfig() {
    return loadConfig(new HashMap<>());
  }

  /**
   * Create the job files to be uploaded into the cluster
   */
  private String prepareJobFiles() {

    String jobJarFile = SchedulerContext.userJobJarFile(config);
    if (jobJarFile == null) {
      throw new RuntimeException("Job file cannot be null");
    }

    // create a temp directory to save the job package
    Path tempDirPath = null;
    String tempDirPrefix = "twister2-" + job.getJobName() + "-";
    try {
      String jobArchiveTemp = SchedulerContext.jobArchiveTempDirectory(config);
      if (jobArchiveTemp != null) {
        tempDirPath = Files.createTempDirectory(Paths.get(jobArchiveTemp), tempDirPrefix);
      } else {
        tempDirPath = Files.createTempDirectory(tempDirPrefix);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temp directory with the prefix: "
          + tempDirPrefix, e);
    }

    // temp directory to put archive files
    String tempDirPathString = tempDirPath.toString();

    // copy the core dist package to temp directory
    // do not copy if its a kubernetes cluster
    if (!SchedulerContext.copySystemPackage(config)) {
      LOG.log(Level.INFO, "No need to copy the systems package");
    } else {
      String twister2CorePackage = SchedulerContext.systemPackageUrl(config);
      if (twister2CorePackage == null) {
        throw new RuntimeException("Core package is not specified in the configuration");
      }
      LOG.log(Level.INFO, String.format("Copy core package: %s to %s",
          twister2CorePackage, tempDirPathString));
      try {
        FileUtils.copyFileToDirectory(twister2CorePackage, tempDirPathString);
      } catch (IOException e) {
        throw new RuntimeException("Failed to copy the core package", e);
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

    // set the job type
    boolean ziped = false;
    String jobType = SchedulerContext.userJobType(config);
    if ("jar".equals(jobType)) {
      format.setType(JobAPI.JobFormatType.JAR);
    } else if ("java_zip".equals(jobType)) {
      format.setType(JobAPI.JobFormatType.JAVA_ZIP);
      ziped = true;
    } else if ("python".equals(jobType)) {
      format.setType(JobAPI.JobFormatType.PYTHON);
    } else if ("python_zip".equals(jobType)) {
      ziped = true;
      format.setType(JobAPI.JobFormatType.PYTHON_ZIP);
    }

    // set the job file
    format.setJobFile(jobJarFileName);
    job = JobAPI.Job.newBuilder(job).setJobFormat(format).build();

    // add job description file to the archive
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(job.getJobId());
    boolean added = packer.addFileToArchive(jobDescFileName, job.toByteArray());
    if (!added) {
      throw new RuntimeException("Failed to add the job description file to the archive: "
          + jobDescFileName);
    }

    // add job jar file to the archive
    if (!ziped) {
      added = packer.addFileToArchive(jobJarFile);
    } else {
      added = packer.addZipToArchive(jobJarFile);
    }
    if (!added) {
      throw new RuntimeException("Failed to add the job jar file to the archive: " + jobJarFile);
    }

    // add conf dir to the archive
    String confDir = SchedulerContext.conf(config);
    added = packer.addDirectoryToArchive(confDir);
    if (!added) {
      throw new RuntimeException("Failed to add the conf dir to the archive: " + confDir);
    }

    String commonConfDir = SchedulerContext.commonConfDir(config);
    added = packer.addDirectoryToArchive(commonConfDir);
    if (!added) {
      throw new RuntimeException("Failed to add the conf dir to the archive: " + commonConfDir);
    }

    // close the archive file
    packer.close();
    LOG.log(Level.INFO, "Archive file created: " + packer.getArchiveFileName());

    // add the job description filename, userJobJar and conf directory to the config
    config = Config.newBuilder()
        .putAll(config)
        .put(SchedulerContext.USER_JOB_FILE, jobJarFileName)
        .put(SchedulerContext.TEMPORARY_PACKAGES_PATH, tempDirPathString)
        .build();

    return tempDirPathString;
  }

  /**
   * Submit the job to the cluster
   */
  public Twister2JobState submitJob() {

    // check whether uploader and launcher classes are specified
    checkUploaderAndLauncherClasses();
    String jobDirectory = prepareJobFiles();

    // upload the job package
    IUploader uploader = uploadJobPackage();

    // initialize the launcher and launch the job
    ILauncher launcher = initializeLauncher();
    Twister2JobState launchState = launcher.launch(job);

    // if the job can not be initialized successfully
    // clear job files and return
    if (!launchState.isRequestGranted()) {
      launcher.close();
      if (!SchedulerContext.isLocalFileSystemUploader(config)) {
        uploader.undo();
      }
      // clear temporary twister2 files
      clearTemporaryFiles(jobDirectory);
      return launchState;
    }

    // when the uploader is threaded, wait for the uploader to complete
    // if it can not complete successfully
    // clear job resources and return
    if (!uploader.complete()) {
      LOG.log(Level.SEVERE, "Transferring the job package failed."
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      launcher.killJob(job.getJobId());
      launchState.setRequestGranted(false);
      launcher.close();
      // clear temporary twister2 files
      clearTemporaryFiles(jobDirectory);
      return launchState;
    }

    // job is submitted successfully
    // close the launcher
    launcher.close();

    // if this is a checkpointed job and the uploader is not LocalFileSystemUploader
    // copy the job package to the local repository
    if (CheckpointingContext.isCheckpointingEnabled(config)
        && !SchedulerContext.isLocalFileSystemUploader(config)) {

      IUploader localUploader = new LocalFileSystemUploader();
      localUploader.initialize(config, job.getJobId());
      URI savedPackage = localUploader.uploadPackage(jobDirectory);
      LOG.info("Saved Job Package to Directory: " + savedPackage.getPath());
    }

    if (!CheckpointingContext.isCheckpointingEnabled(config)
        && SchedulerContext.clusterType(config).equals("standalone")
        && SchedulerContext.isLocalFileSystemUploader(config)) {
      uploader.undo();
    }

    // clear temporary twister2 files
    clearTemporaryFiles(jobDirectory);

    return launchState;
  }

  /**
   * Resubmit the job to the cluster
   */
  public Twister2JobState resubmitJob() {

    // check whether uploader and launcher classes are specified
    checkUploaderAndLauncherClasses();

    // upload the job package if it is not local upoader
    IUploader uploader = null;
    if (!SchedulerContext.isLocalFileSystemUploader(config)) {
      uploader = uploadJobPackage();
    }

    // initialize the launcher and launch the job
    ILauncher launcher = initializeLauncher();
    Twister2JobState launchState = launcher.launch(job);

    // if the job can not be initialized successfully
    // clear job files and return
    if (!launchState.isRequestGranted()) {
      launcher.close();
      if (uploader != null) {
        uploader.undo();
      }
      return launchState;
    }

    // when the uploader is threaded, wait for the uploader to complete
    // if it can not complete successfully
    // clear job resources and return
    if (uploader != null && !uploader.complete()) {
      LOG.log(Level.SEVERE, "Transferring the job package failed."
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      launcher.killJob(job.getJobId());
      launchState.setRequestGranted(false);
      launcher.close();
      return launchState;
    }

    // job is submitted successfully
    // close the launcher
    launcher.close();

    return launchState;
  }

  private void checkUploaderAndLauncherClasses() {

    String uploaderClass = SchedulerContext.uploaderClass(config);
    if (uploaderClass == null) {
      throw new RuntimeException("The uploader class must be specified");
    }
  }

  private IUploader uploadJobPackage() {

    String uploaderClass = SchedulerContext.uploaderClass(config);
    IUploader uploader;
    ClassLoader classLoader = ResourceAllocator.class.getClassLoader();
    // create an instance of uploader
    try {
      uploader = ReflectionUtils.newInstance(classLoader, uploaderClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new UploaderException(
          String.format("Failed to instantiate uploader class '%s'", uploaderClass), e);
    }

    LOG.fine("Initialize uploader");
    // now upload the content of the package
    uploader.initialize(config, job.getJobId());
    // gives the url of the file to be uploaded
    LOG.fine("Calling uploader to upload the job package");
    long start = System.currentTimeMillis();
    String jobDirectory = SchedulerContext.temporaryPackagesPath(config);
    URI packageURI = uploader.uploadPackage(jobDirectory);
    long delay = System.currentTimeMillis() - start;
    LOG.info("Job package upload started. It took: " + delay + "ms");

    // add scp address as a prefix to returned URI: user@ip
    String scpServerAdress = ScpContext.scpConnection(config);
    String scpPath = scpServerAdress;
    if (packageURI != null) {
      scpPath += ":" + packageURI.toString() + "/";
    }
    LOG.fine("SCP PATH to copy files from: " + scpPath);

    // now launch the launcher
    // Update the runtime config with the packageURI
    config = Config.newBuilder()
        .putAll(config)
        .put(SchedulerContext.TWISTER2_PACKAGES_PATH, scpPath)
        .put(SchedulerContext.JOB_PACKAGE_URI, packageURI)
        .build();

    return uploader;
  }

  private ILauncher initializeLauncher() {

    String launcherClass = SchedulerContext.launcherClass(config);
    ILauncher launcher;
    ClassLoader classLoader = ResourceAllocator.class.getClassLoader();

    // create an instance of launcher
    try {
      launcher = ReflectionUtils.newInstance(classLoader, launcherClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new LauncherException(
          String.format("Failed to instantiate launcher class '%s'", launcherClass), e);
    }

    // initialize the launcher
    launcher.initialize(config);
    return launcher;
  }

  /**
   * clear temporary files
   *
   * @param jobDirectory the name of the folder to be cleaned
   */
  public void clearTemporaryFiles(String jobDirectory) {

    // job package may need to be uploaded to failed workers or newly scaled workers
    // so, we do not delete the job package
    if (Context.isKubernetesCluster(config)
        && SchedulerContext.uploaderClass(config)
        .equals("edu.iu.dsc.tws.rsched.uploaders.k8s.K8sUploader")
        && RequestObjectBuilder.uploadMethod.equals("client-to-pods")) {

      return;
    }

    Path jobPackagePath = Paths.get(jobDirectory);
    if (Files.notExists(jobPackagePath)) {
      LOG.severe("Job Package directory does not exist: " + jobDirectory);
    } else {
      FileUtils.deleteDir(jobDirectory);
      LOG.log(Level.INFO, "CLEANED TEMPORARY DIRECTORY......:" + jobDirectory);
    }
  }

  /**
   * Kill the job
   *
   * @param jobID of the job to kill
   */
  public static void killJob(String jobID, Config cnfg) {

    String launcherClass = SchedulerContext.launcherClass(cnfg);
    if (launcherClass == null) {
      throw new RuntimeException("The launcher class must be specified");
    }

    ILauncher launcher;
    ClassLoader classLoader = ResourceAllocator.class.getClassLoader();

    // create an instance of launcher
    try {
      launcher = ReflectionUtils.newInstance(classLoader, launcherClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new LauncherException(
          String.format("Failed to instantiate launcher class '%s'", launcherClass), e);
    }

    // initialize the launcher and terminate the job
    launcher.initialize(cnfg);
    boolean killed = launcher.killJob(jobID);
    if (!killed) {
      LOG.log(Level.SEVERE, "Could not kill the job");
    }

    String uploaderClass = SchedulerContext.uploaderClass(cnfg);
    if (uploaderClass == null) {
      throw new RuntimeException("The uploader class must be specified");
    }

    IUploader uploader;
    // create an instance of uploader
    try {
      uploader = ReflectionUtils.newInstance(classLoader, uploaderClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new UploaderException(
          String.format("Failed to instantiate uploader class '%s'", uploaderClass), e);
    }

    uploader.initialize(cnfg, jobID);
    uploader.undo();

    launcher.close();
    uploader.close();

    if (KubernetesContext.isKubernetesCluster(cnfg)) {
      KubernetesController.close();
    }

    if (CheckpointingContext.isCheckpointingEnabled(cnfg)
        && !SchedulerContext.uploaderClass(cnfg)
        .equals("edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader")) {
      IUploader localUploader = new LocalFileSystemUploader();
      localUploader.initialize(cnfg, jobID);
      localUploader.undo();
    }
  }

}
