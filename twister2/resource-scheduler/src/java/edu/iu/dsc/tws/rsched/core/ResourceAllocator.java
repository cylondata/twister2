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
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.scheduler.ILauncher;
import edu.iu.dsc.tws.api.scheduler.IUploader;
import edu.iu.dsc.tws.api.scheduler.LauncherException;
import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import edu.iu.dsc.tws.api.scheduler.UploaderException;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;
import edu.iu.dsc.tws.rsched.uploaders.scp.ScpContext;
import edu.iu.dsc.tws.rsched.utils.FileUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;
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

  private JobAPI.Job updatedJob;
  private Config updatedConfig;

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

    Config config = ConfigLoader.loadConfig(twister2Home, configDir, clusterType);

    // set log level
    // LoggingHelper.setLogLevel(LoggingContext.loggingLevel(config));

    LOG.log(Level.INFO, String.format("Loaded configuration with twister2_home: %s and "
        + "configuration: %s and cluster: %s", twister2Home, configDir, clusterType));

    return Config.newBuilder().
        putAll(config).
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
  private String prepareJobFiles(Config config, JobAPI.Job job) {

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
    updatedJob = JobAPI.Job.newBuilder(job).setJobFormat(format).build();

    // add job description file to the archive
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(job.getJobId());
    boolean added = packer.addFileToArchive(jobDescFileName, updatedJob.toByteArray());
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
    updatedConfig = Config.newBuilder()
        .putAll(config)
        .put(SchedulerContext.USER_JOB_FILE, jobJarFileName)
        .put(SchedulerContext.TEMPORARY_PACKAGES_PATH, tempDirPathString)
        .build();

    return tempDirPathString;
  }

  /**
   * Submit the job to the cluster
   *
   * @param job the actual job description
   */
  public Twister2JobState submitJob(JobAPI.Job job, Config config) {
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
      launcher = ReflectionUtils.newInstance(ResourceAllocator.class.getClassLoader(),
          launcherClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new LauncherException(
          String.format("Failed to instantiate launcher class '%s'", launcherClass), e);
    }

    // create an instance of uploader
    try {
      uploader = ReflectionUtils.newInstance(ResourceAllocator.class.getClassLoader(),
          uploaderClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new UploaderException(
          String.format("Failed to instantiate uploader class '%s'", uploaderClass), e);
    }

    LOG.fine("Initialize uploader");
    // now upload the content of the package
    uploader.initialize(updatedConfig, updatedJob);
    // gives the url of the file to be uploaded
    LOG.fine("Calling uploader to upload the job package");
    long start = System.currentTimeMillis();
    URI packageURI = uploader.uploadPackage(jobDirectory);
    long delay = System.currentTimeMillis() - start;
    LOG.info("Job package upload started. It took: " + delay + "ms");

    // add scp address as a prefix to returned URI: user@ip
    String scpServerAdress = ScpContext.scpConnection(updatedConfig);
    String scpPath = scpServerAdress;
    if (packageURI != null) {
      scpPath += ":" + packageURI.toString() + "/";
    }
    LOG.fine("SCP PATH to copy files from: " + scpPath);

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

    start = System.currentTimeMillis();
    Twister2JobState state = launcher.launch(updatedJob);
    long end =  System.currentTimeMillis();
    delay = end - start;
    LOG.info("Job launching took: " + delay + "ms");

//    String dir = System.getProperty("user.home") + "/.twister2";
//    String delayFile = dir + "/" + job.getJobId() + "-launch-delay.txt";
//    String ts = (String) updatedConfig.get("JOB_SUBMIT_TIME");
//    long jsDelay = end - Long.parseLong(ts);
//    FileUtils.writeToFile(delayFile, (jsDelay + "").getBytes(), true);
//    LOG.info("Job launch delay: " + jsDelay + " ms");

    launcher.close();

    // when the job initialized successfully
    // complete uploading if it is threaded
    // otherwise, undo uploading
    if (state.isRequestGranted()) {
      boolean transferred = uploader.complete();

      if (!transferred) {
        LOG.log(Level.SEVERE, "Transferring the job package failed."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        launcher.terminateJob(job.getJobId());
        state.setRequestGranted(false);
      }
    } else {
      uploader.undo(updatedConfig, job.getJobId());
    }

    if (SchedulerContext.clusterType(updatedConfig).equals("kubernetes")
        && SchedulerContext.uploaderClass(updatedConfig)
        .equals("edu.iu.dsc.tws.rsched.uploaders.k8s.K8sUploader")
        && RequestObjectBuilder.uploadMethod.equals("client-to-pods")
        && JobUtils.isJobScalable(updatedConfig, updatedJob)) {

      // job package may need to be uploaded to newly scaled workers
      // so, we do not delete the job package

    } else {
      clearTemporaryFiles(jobDirectory);
    }

    return state;
  }

  /**
   * clear temporary files
   *
   * @param jobDirectory the name of the folder to be cleaned
   */
  public void clearTemporaryFiles(String jobDirectory) {

    String cleaningCommand = "rm -rf " + jobDirectory;
    System.out.println("cleaning  command:" + cleaningCommand);
    ProcessUtils.runSyncProcess(false,
        cleaningCommand.split(" "), new StringBuilder(),
        new File("."), true);

    LOG.log(Level.INFO, "CLEANED TEMPORARY DIRECTORY......:" + jobDirectory);
  }


  /**
   * Terminate a job
   *
   * @param jobID the name of the job to terminate
   */
  public void terminateJob(String jobID, Config config) {

    String launcherClass = SchedulerContext.launcherClass(config);
    if (launcherClass == null) {
      throw new RuntimeException("The launcher class must be specified");
    }

    ILauncher launcher;

    // create an instance of launcher
    try {
      launcher = ReflectionUtils.newInstance(ResourceAllocator.class.getClassLoader(),
          launcherClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new LauncherException(
          String.format("Failed to instantiate launcher class '%s'", launcherClass), e);
    }

    // initialize the launcher and terminate the job
    launcher.initialize(config);
    boolean terminated = launcher.terminateJob(jobID);
    if (!terminated) {
      LOG.log(Level.SEVERE, "Could not terminate the job");
    }

    launcher.close();

    String uploaderClass = SchedulerContext.uploaderClass(config);
    if (uploaderClass == null) {
      throw new RuntimeException("The uploader class must be specified");
    }

    IUploader uploader;
    // create an instance of uploader
    try {
      uploader = ReflectionUtils.newInstance(ResourceAllocator.class.getClassLoader(),
          uploaderClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new UploaderException(
          String.format("Failed to instantiate uploader class '%s'", uploaderClass), e);
    }

    uploader.undo(config, jobID);
    uploader.close();
  }

}
