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
package edu.iu.dsc.tws.rsched.schedulers.k8s.worker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.proto.system.ResourceAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesField;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.spi.container.IPersistentVolume;
import edu.iu.dsc.tws.rsched.spi.container.IWorker;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.TarGzipPacker;

import static edu.iu.dsc.tws.common.config.Context.DIR_PREFIX_FOR_JOB_ARCHIVE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.KUBERNETES_CLUSTER_TYPE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

public final class KubernetesWorker {
  private static final Logger LOG = Logger.getLogger(KubernetesWorker.class.getName());

  public static final String UNPACK_COMPLETE_FILE_NAME = "unpack-complete.txt";
  public static final long FILE_WAIT_SLEEP_INTERVAL = 30;
  public static final int PERSISTENT_DIR_CREATE_TRY_COUNT = 3; // ms

  public static Config config = null;
  public static WorkerNetworkInfo thisWorker;
  public static JobMasterClient jobMasterClient;

  private KubernetesWorker() {
  }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    Config envConfigs = buildConfigFromEnvVariables();

    StringBuffer logBuffer = new StringBuffer();
    // get the remaining environment variables that are not included in Config object
    String userJobJarFile = System.getenv(KubernetesField.USER_JOB_JAR_FILE + "");
    logBuffer.append(KubernetesField.USER_JOB_JAR_FILE + ": " + userJobJarFile + "\n");

    String fileSizeStr = System.getenv(KubernetesField.JOB_PACKAGE_FILE_SIZE + "");
    logBuffer.append(KubernetesField.JOB_PACKAGE_FILE_SIZE + ": " + fileSizeStr + "\n");

    String containerName = System.getenv(KubernetesField.CONTAINER_NAME + "");
    logBuffer.append(KubernetesField.CONTAINER_NAME + ": " + containerName + "\n");

    String podIP = System.getenv(KubernetesField.POD_IP + "");
    logBuffer.append(KubernetesField.POD_IP + ": " + podIP + "\n");

    String persistentJobDir = System.getenv(KubernetesField.PERSISTENT_JOB_DIR + "").trim();
    logBuffer.append(KubernetesField.PERSISTENT_JOB_DIR + ": " + persistentJobDir + "\n");

    // this environment variable is not sent by submitting client, it is set by Kubernetes master
    String podName = System.getenv("HOSTNAME");
    logBuffer.append("POD_NAME(HOSTNAME): " + podName + "\n");

    // set workerID
    int containersPerPod = KubernetesContext.workersPerPod(envConfigs);
    int workerID = calculateWorkerID(podName, containerName, containersPerPod);

    // set thisWorker variable
    int workerPort = KubernetesContext.workerPort(envConfigs);
    thisWorker =
        new WorkerNetworkInfo(KubernetesUtils.convertToIPAddress(podIP), workerPort, workerID);

    K8sPersistentVolume pv = null;
    // create persistent job dir if there is a persistent volume request
    if (persistentJobDir == null || persistentJobDir.isEmpty()) {
      // no persistent volume is requested, nothing to be done
    } else {
      createPersistentJobDir(podName, containerName, persistentJobDir, 0);

      // create persistent volume object
      pv = new K8sPersistentVolume(persistentJobDir, workerID);
    }

    // initialize the logger file
    initLogger(workerID, pv, envConfigs);

    LOG.info("KubernetesWorker started. Current time: " + System.currentTimeMillis());
    LOG.info("Received parameters as environment variables: \n"
        + logBuffer.toString() + "\n" + envConfigs);

    // log persistent volume related messages
    if (pv == null) {
      LOG.info("No persistent volume is requested. ");
    } else {
      StringBuffer pvInfo = new StringBuffer();
      pvInfo.append("Persistent storage information: \n");
      pvInfo.append("Job Dir Path: " + pv.getJobDirPath() + "\n");
      pvInfo.append("Worker Dir Path: " + pv.getWorkerDirPath() + "\n");
      pvInfo.append("Job log dir: " + pv.getLogDirPath());
      LOG.info(pvInfo.toString());
    }

    // start WorkerController to talk to the job master
    startJobMasterClient(envConfigs);

    // construct relevant variables from environment variables
    // job package can be either in pod shared drive or in persistent volume
    String jobPackageFileName = SchedulerContext.jobPackageFileName(envConfigs);
    long fileSize = Long.parseLong(fileSizeStr);
    if (pv != null && KubernetesContext.persistentVolumeUploading(envConfigs)) {
      jobPackageFileName = persistentJobDir + "/" + jobPackageFileName;
    } else {
      jobPackageFileName = POD_MEMORY_VOLUME + "/" + jobPackageFileName;
    }
    String jobDescFileName = SchedulerContext.jobDescriptionFile(envConfigs);
    userJobJarFile = POD_MEMORY_VOLUME + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE + userJobJarFile;
    jobDescFileName = POD_MEMORY_VOLUME + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE + jobDescFileName;
    String configDir = POD_MEMORY_VOLUME + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE
        + KUBERNETES_CLUSTER_TYPE;

    boolean ready = waitUnpack(containerName, jobPackageFileName, fileSize);
    if (!ready) {
      return;
    }

    boolean loaded = loadLibrary(userJobJarFile);
    if (!loaded) {
      return;
    }

    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFileName);
    LOG.info("Job description file is read: " + jobDescFileName);

    // load config from config dir
    Config fileConfigs = loadConfig(configDir);
    // override some config from job object if any
    config = overrideConfigs(job, fileConfigs, envConfigs);

    LOG.fine("Loaded config values: \n" + config.toString());

    // start worker controller
//    WorkerController workerController =
//        new WorkerController(config, podName, podIP, containerName, job.getJobName());
//    thisWorker = workerController.getWorkerNetworkInfo();

    ResourceAPI.ComputeResource cr = job.getJobResources().getContainer();

    jobMasterClient.sendWorkerRunningMessage();
    startWorkerClass(jobMasterClient.getWorkerController(), pv);
    closeWorker(podName);
  }

  /**
   * construct a Config object from environment variables
   * @return
   */
  public static Config buildConfigFromEnvVariables() {
    return Config.newBuilder()
        .put(SchedulerContext.JOB_PACKAGE_FILENAME,
            System.getenv(SchedulerContext.JOB_PACKAGE_FILENAME))
        .put(SchedulerContext.JOB_DESCRIPTION_FILE,
            System.getenv(SchedulerContext.JOB_DESCRIPTION_FILE))
        .put(KubernetesContext.WORKERS_PER_POD, System.getenv(KubernetesContext.WORKERS_PER_POD))
        .put(LoggingContext.PERSISTENT_LOGGING_REQUESTED,
            System.getenv(LoggingContext.PERSISTENT_LOGGING_REQUESTED))
        .put(LoggingContext.LOGGING_LEVEL, System.getenv(LoggingContext.LOGGING_LEVEL))
        .put(LoggingContext.REDIRECT_SYS_OUT_ERR,
            System.getenv(LoggingContext.REDIRECT_SYS_OUT_ERR))
        .put(LoggingContext.MAX_LOG_FILE_SIZE, System.getenv(LoggingContext.MAX_LOG_FILE_SIZE))
        .put(LoggingContext.MAX_LOG_FILES, System.getenv(LoggingContext.MAX_LOG_FILES))
        .put(KubernetesContext.PERSISTENT_VOLUME_UPLOADING,
            System.getenv(KubernetesContext.PERSISTENT_VOLUME_UPLOADING))
        .put(KubernetesContext.K8S_WORKER_PORT, System.getenv(KubernetesContext.K8S_WORKER_PORT))
        .put(JobMasterContext.JOB_MASTER_IP, System.getenv(JobMasterContext.JOB_MASTER_IP))
        .put(JobMasterContext.JOB_MASTER_PORT, System.getenv(JobMasterContext.JOB_MASTER_PORT))
        .put(Context.TWISTER2_WORKER_INSTANCES, System.getenv(Context.TWISTER2_WORKER_INSTANCES))
        .put(JobMasterContext.PING_INTERVAL, System.getenv(JobMasterContext.PING_INTERVAL))
        .put(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS,
            System.getenv(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS))
        .put(JobMasterContext.WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION,
            System.getenv(JobMasterContext.WORKER_TO_JOB_MASTER_RESPONSE_WAIT_DURATION))
        .build();
  }

  public static void startJobMasterClient(Config cnfg) {

    jobMasterClient = new JobMasterClient(cnfg, thisWorker);
    jobMasterClient.init();
    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();
  }

  /**
   * itinialize the logger
   * @param workerID
   * @param pv
   * @param cnfg
   */
  public static void initLogger(int workerID, K8sPersistentVolume pv, Config cnfg) {
    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cnfg));

    // if persistent logging is requested, initialize it
    if (pv != null && LoggingContext.persistentLoggingRequested(cnfg)) {

      if (LoggingContext.redirectSysOutErr(cnfg)) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
            + "Check the log file for the upcoming log messages. ");
      }

      LoggingHelper.setupLogging(cnfg, pv.getLogDirPath(),
          K8sPersistentVolume.LOG_FILE_NAME_PREFIX + workerID);

      LOG.info("Persistent logging to file initialized.");
    }
  }

  /**
   * last method to call to close the worker
   */
  public static void closeWorker(String podName) {

    // send worker completed message to the Job Master and wait for job master to delete the worker
    jobMasterClient.sendWorkerCompletedMessage();
    jobMasterClient.close();
    waitIndefinitely();
  }

  /**
   * a test method to make the worker wait indefinitely
   */
  public static void waitIndefinitely() {

    while (true) {
      try {
        LOG.info("Worker completed. Waiting idly to be deleted by Job Master. Sleeping 100sec. "
            + "Time: " + new java.util.Date());
        Thread.sleep(100000);
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }
  }

  /**
   * calculate the workerID from the given parameters
   */
  public static int calculateWorkerID(String podName, String containerName, int workersPerPod) {
    int podNo = KubernetesUtils.idFromName(podName);
    int containerIndex = KubernetesUtils.idFromName(containerName);

    return podNo * workersPerPod + containerIndex;
  }

  /**
   * update the count in the shared file with a lock
   * to let other workers in this pod to know that a worker has finished
   * currently not used
   */
  public static int updateCompletions() {

    String completionsFile = POD_MEMORY_VOLUME + "/completions.txt";

    try {
      Path path = Paths.get(completionsFile);
      FileChannel fileChannel = FileChannel.open(path,
          StandardOpenOption.WRITE, StandardOpenOption.READ);
      LOG.info("Opened File channel. Acquiring lock ...");

      FileLock lock = fileChannel.lock(); // exclusive lock
      LOG.info("Acquired the file lock. Validity of the lock: " + lock.isValid());

      // read the counter from the file
      ByteBuffer buffer = ByteBuffer.allocate(20);
      int noOfBytesRead = fileChannel.read(buffer);
      byte[] readByteArray = buffer.array();
      String inStr = new String(readByteArray, 0, noOfBytesRead, StandardCharsets.UTF_8);
      int count = Integer.parseInt(inStr);

      // update the counter and write back to the file
      count++;
      String outStr = Integer.toString(count);
      byte[] outByteArray = outStr.getBytes(StandardCharsets.UTF_8);
      ByteBuffer outBuffer = ByteBuffer.wrap(outByteArray);
      fileChannel.write(outBuffer, 0);
      LOG.info("Counter in file [" + completionsFile + "] updated to: " + count);

      // close the file channel and release the lock
      fileChannel.close(); // also releases the lock

      return count;

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when updating the counter in file: " + completionsFile, e);
      return -1;
    }
  }

  /**
   * start the container class specified in conf files
   */
  public static void startWorkerClass(IWorkerController workerController,
                                      IPersistentVolume pv) {
    String containerClass = SchedulerContext.containerClass(config);
    IWorker container;
    try {
      Object object = ReflectionUtils.newInstance(containerClass);
      container = (IWorker) object;
      LOG.info("loaded worker class: " + containerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.severe(String.format("failed to load the container class %s", containerClass));
      throw new RuntimeException(e);
    }

    K8sVolatileVolume volatileVolume = null;
    if (SchedulerContext.volatileDiskRequested(config)) {
      volatileVolume =
          new K8sVolatileVolume(SchedulerContext.jobName(config), thisWorker.getWorkerID());
    }
    container.init(config, thisWorker.getWorkerID(), null, workerController, pv, volatileVolume);
  }


  /**
   * configs from job object will override the ones from config files,
   * the configs from environment variables overrides all
   */
  public static Config overrideConfigs(JobAPI.Job job, Config fileConfigs, Config envConfigs) {

    Config.Builder builder = Config.newBuilder().putAll(fileConfigs);

    JobAPI.Config conf = job.getConfig();
    LOG.info("Number of configs to override from job conf: " + conf.getKvsCount());

    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      builder.put(kv.getKey(), kv.getValue());
      LOG.info("Overriden conf key-value pair: " + kv.getKey() + ": " + kv.getValue());
    }

    builder.putAll(envConfigs);

    return builder.build();
  }

  /**
   * loadConfig from config files
   */
  public static Config loadConfig(String configDir) {

    // first lets read the essential properties from java system properties
    String twister2Home = Paths.get("").toAbsolutePath().toString();

    LOG.log(Level.INFO, String.format("Loading configuration with twister2_home: %s and "
        + "configuration: %s", twister2Home, configDir));
    Config conf1 = ConfigLoader.loadConfig(twister2Home, configDir);
    Config conf2 = Config.newBuilder().
        putAll(conf1).
        put(Context.TWISTER2_HOME.getKey(), twister2Home).
        put(Context.TWISTER2_CONF.getKey(), configDir).
        put(Context.TWISTER2_CLUSTER_TYPE, KUBERNETES_CLUSTER_TYPE).
        build();

    LOG.log(Level.INFO, "Config files are read from directory: " + configDir);
    return conf2;
  }


  /**
   * Load a jar file dynamically
   * <p>
   * This method is copied from:
   * https://stackoverflow.com/questions/27187566/load-jar-dynamically-at-runtime
   */
  @SuppressWarnings("rawtypes")
  public static boolean loadLibrary(String jarFile) {
    try {
      File jar = new File(jarFile);
      /*We are using reflection here to circumvent encapsulation; addURL is not public*/
      java.net.URLClassLoader loader = (java.net.URLClassLoader) ClassLoader.getSystemClassLoader();
      java.net.URL url = jar.toURI().toURL();
      /*Disallow if already loaded*/
      for (java.net.URL it : java.util.Arrays.asList(loader.getURLs())) {
        if (it.equals(url)) {
          return true;
        }
      }
      java.lang.reflect.Method method =
          java.net.URLClassLoader.class.getDeclaredMethod("addURL",
              new Class[]{java.net.URL.class});
      method.setAccessible(true); /*promote the method to public access*/
      method.invoke(loader, new Object[]{url});

      LOG.info("The jar file is loaded: " + jarFile);
      return true;

    } catch (final java.lang.NoSuchMethodException
        | java.lang.IllegalAccessException
        | java.net.MalformedURLException
        | java.lang.reflect.InvocationTargetException e) {
      LOG.log(Level.SEVERE, "Exception when loading the jar file: " + jarFile, e);
      return false;
    }
  }

  public static boolean waitUnpack(String containerName, String jobPackageFileName, long fileSize) {

    String flagFileName = POD_MEMORY_VOLUME + "/" + UNPACK_COMPLETE_FILE_NAME;
//    String completionsFileName = POD_MEMORY_VOLUME + "/completions.txt";

    // if it is the first container in a pod, unpack the tar.gz file
    if (containerName.endsWith("-0")) {

      boolean transferred = waitForFileTransfer(jobPackageFileName, fileSize);
      if (transferred) {
        File outputDir = new File(POD_MEMORY_VOLUME);
        boolean jobFileUnpacked = TarGzipPacker.unpack(jobPackageFileName, outputDir);
        if (jobFileUnpacked) {
          LOG.info("Job file [" + jobPackageFileName + "] unpacked successfully.");
          boolean written1 = writeFile(flagFileName, 0);
          return written1;
//          boolean written2 = writeFile(completionsFileName, 0);
//          return written1 && written2;
        } else {
          LOG.severe("Job file can not be unpacked.");
          return false;
        }
      } else {
        LOG.severe("Something went wrong with receiving job file.");
        return false;
      }

    } else {
      return waitForFlagFile(flagFileName);
    }
  }

  /**
   * write a file with an int in it.
   */
  public static boolean writeFile(String fileName, int number) {
    try {
      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(fileName), StandardCharsets.UTF_8));
      writer.write(Integer.toString(number));
      writer.flush();

      //Close writer
      writer.close();

      LOG.info("File: " + fileName + " is written.");
      return true;

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when writing the file: " + fileName, e);
      return false;
    }
  }


  /**
   * Wait for the job package file to be transferred to this pod
   */
  public static boolean waitForFileTransfer(String jobFileName, long fileSize) {

    boolean transferred = false;
    File jobFile = new File(jobFileName);

    // when waiting, it will print log messages at least after this much time
    long logMessageInterval = 1000;
    //this count is restarted after each log message
    long waitTimeCountForLog = 0;

    while (!transferred) {
      if (jobFile.exists()) {
        // if the file is fully received
        if (fileSize == jobFile.length()) {
          LOG.info("Job File [" + jobFileName + "] is fully received.");
          return true;

          // if the file is being received. Transmission started.
        } else if (fileSize > jobFile.length() && waitTimeCountForLog >= logMessageInterval) {
          LOG.info("Job File [" + jobFileName + "] is being transferred. Current file size: "
              + jobFile.length());
          waitTimeCountForLog = 0;

          // received file size is larger than it is supposed to be. Something wrong.
        } else if (fileSize < jobFile.length()) {
          LOG.info("Job File [" + jobFileName + "] size is larger than it supposed to be. Aborting."
              + "Current file size: " + jobFile.length());
          return false;
        }

        // waiting. file transfer has not started yet.
      } else if (waitTimeCountForLog >= logMessageInterval) {
        LOG.info("Job File [" + jobFileName + "] is not started to be received yet. Waiting.");
        waitTimeCountForLog = 0;
      }

      try {
        Thread.sleep(FILE_WAIT_SLEEP_INTERVAL);
        waitTimeCountForLog += FILE_WAIT_SLEEP_INTERVAL;
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }

    return false;
  }

  /**
   * The workers except the one in the first container in a pod wait for
   * the first worker to write the unpack-complete.txt file
   */
  public static boolean waitForFlagFile(String flagFileName) {

    boolean flagFileCreated = false;
    File flagFile = new File(flagFileName);

    // when waiting, it will print log message at least after this much time
    long logMessageInterval = 1000;
    //this count is restarted after each log message
    long waitTimeCountForLog = 0;

    while (!flagFileCreated) {
      if (flagFile.exists()) {
        LOG.info("Flag file is ready: " + flagFileName + ". Will start processing container.");
        return true;
      } else if (waitTimeCountForLog >= logMessageInterval) {
        LOG.info("Flag File does not exist yet. Waiting " + logMessageInterval + "ms");
        waitTimeCountForLog = 0;
      }

      try {
        Thread.sleep(FILE_WAIT_SLEEP_INTERVAL);
        waitTimeCountForLog += FILE_WAIT_SLEEP_INTERVAL;
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }

    return false;
  }

  public static boolean createPersistentJobDirIfFirstWorker(
      String podName, String containerName, String persistentJobDir) {

    // check whether this is the worker 0
    int podIndex = KubernetesUtils.idFromName(podName);
    int containerIndex = KubernetesUtils.idFromName(containerName);
    if (podIndex == 0 && containerIndex == 0) {
      File persistentDir = new File(persistentJobDir);
      if (persistentDir.exists()) {
        LOG.severe("Persistent job dir [" + persistentJobDir
            + "] already exist. Something must be wrong. ");
        return false;
      } else {
        boolean dirCreated = persistentDir.mkdirs();
        if (dirCreated) {
          LOG.info("Persistent job dir [" + persistentJobDir + "] created.");
          return true;
        } else {
          LOG.severe("Persistent job dir [" + persistentJobDir + "] can not be created.");
          return false;
        }
      }
    }

    // if it is not the first worker, do nothing
    return true;
  }

  /**
   * which ever worker comes first to this point, it will create the job dir
   */
  public static boolean createPersistentJobDir(
      String podName, String containerName, String persistentJobDir, int attemptNo) {

    if (attemptNo == PERSISTENT_DIR_CREATE_TRY_COUNT) {
      return false;
    }

    File persistentDir = new File(persistentJobDir);
    if (persistentDir.exists()) {
      // another worker has already created it, return with success
      return true;
    } else {
      boolean dirCreated = persistentDir.mkdirs();
      if (dirCreated) {
//        LOG.info("Persistent job dir [" + persistentJobDir + "] created by the worker "
//            + "at pod: " + podName + " and on the container: " + containerName);
        return true;
      } else {

        // more than one worker may have attempted to create the dir and it may have failed
        // for this worker but it may have succeeded for another worker.
        // so the dir may have been created. check it again
        if (persistentDir.exists()) {
          return true;
        } else {
          // sleep some time and try again
          LOG.severe("Failed creating persistent dir [" + persistentJobDir + "]. Sleeping and "
              + " will try again.");
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
          }

          return createPersistentJobDir(podName, containerName, persistentJobDir, attemptNo + 1);
        }
      }
    }
  }

}
