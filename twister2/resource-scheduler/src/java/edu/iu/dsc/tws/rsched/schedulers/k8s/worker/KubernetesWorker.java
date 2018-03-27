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

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesField;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import static edu.iu.dsc.tws.common.config.Context.DIR_PREFIX_FOR_JOB_ARCHIVE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.KUBERNETES_CLUSTER_TYPE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_SHARED_VOLUME;

public final class KubernetesWorker {
  private static final Logger LOG = Logger.getLogger(KubernetesWorker.class.getName());

  public static final String UNPACK_COMPLETE_FILE_NAME = "unpack-complete.txt";
  public static final String COMPLETIONS_FILE_NAME = "completions.txt";
  public static final long FILE_WAIT_SLEEP_INTERVAL = 30;

  public static Config config = null;
  public static int containerID = -1; // initially set to an invalid value

  private KubernetesWorker() { }

  public static void main(String[] args) {

    // first get environment variable values
    String jobPackageFileName = System.getenv(KubernetesField.JOB_PACKAGE_FILENAME + "");
    LOG.info(KubernetesField.JOB_PACKAGE_FILENAME + ": " + jobPackageFileName);

    String userJobJarFile = System.getenv(KubernetesField.USER_JOB_JAR_FILE + "");
    LOG.info(KubernetesField.USER_JOB_JAR_FILE + ": " + userJobJarFile);

    String jobDescFileName = System.getenv(KubernetesField.JOB_DESCRIPTION_FILE + "");
    LOG.info(KubernetesField.JOB_DESCRIPTION_FILE + ": " + jobDescFileName);

    String fileSizeStr = System.getenv(KubernetesField.JOB_PACKAGE_FILE_SIZE + "");
    LOG.info(KubernetesField.JOB_PACKAGE_FILE_SIZE + ": " + fileSizeStr);

    String containerName = System.getenv(KubernetesField.CONTAINER_NAME + "");
    LOG.info(KubernetesField.CONTAINER_NAME + ": " + containerName);

    // this environment variable is not sent by submitting client, it is set by Kubernetes master
    String podName = System.getenv("HOSTNAME");
    LOG.info("POD_NAME(HOSTNAME): " + podName);

    // construct relevant variables from environment variables
    long fileSize = Long.parseLong(fileSizeStr);
    jobPackageFileName = POD_SHARED_VOLUME + "/" + jobPackageFileName;
    userJobJarFile = POD_SHARED_VOLUME + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE + userJobJarFile;
    jobDescFileName = POD_SHARED_VOLUME + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE + jobDescFileName;
    containerID = Integer.parseInt(containerName.substring(containerName.lastIndexOf("-") + 1));
    String configDir = POD_SHARED_VOLUME + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE
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
    config = loadConfig(configDir);
    // override some config from job object if any
    config = overrideConfigsFromJob(job, config);

    System.out.println("Loaded config values: ");
    System.out.println(config.toString());

    startContainerClass();

    closeWorker(podName);
  }

  /**
   * last method to call to close the worker
   * @param podName
   */
  public static void closeWorker(String podName) {
    int containersPerPod = KubernetesContext.containersPerPod(config);

    // if this is the only container in a pod, delete the pod and exit
    if (containersPerPod == 1) {
      deletePod(podName);
      return;
    }

    int finishedWorkers = updateCompletions();
    // if there is a problem updating the counter in the file, exit
    if (finishedWorkers == -1) {
      return;
      // if this is not the last worker, just exit
    } else if (containersPerPod > (finishedWorkers + 1)) {
      return;
      // if this is the last worker, delete the pod
    } else if (containersPerPod == (finishedWorkers + 1)) {
      deletePod(podName);
      return;
    }
  }

  public static void deletePod(String podName) {
    String namespace = KubernetesContext.namespace(config);
  }


  /**
   * update the count in the shared file with a lock
   * to let other workers in this pod to know that a worker has finished
   * @return
   */
  public static int updateCompletions() {

    String completionsFile = POD_SHARED_VOLUME + "/" + COMPLETIONS_FILE_NAME;

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
//      System.out.print("Closing the channel and releasing lock.");

      return count;

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when updating the counter in file: " + completionsFile, e);
      return -1;
    }
  }

  /**
   * start the container class specified in conf files
   */
  public static void startContainerClass() {
    String containerClass = SchedulerContext.containerClass(config);
//    String containerClass = "edu.iu.dsc.tws.examples.basic.BasicK8sContainer";
    IContainer container;
    try {
      Object object = ReflectionUtils.newInstance(containerClass);
      container = (IContainer) object;
      LOG.info("loaded container class: " + containerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the container class %s",
          containerClass), e);
      throw new RuntimeException(e);
    }

    container.init(config, containerID, null);
  }


  /**
   * configs from job object will override the ones in config from files if any
   * @return
   */
  public static Config overrideConfigsFromJob(JobAPI.Job job, Config cnfg) {

    Config.Builder builder = Config.newBuilder().putAll(cnfg);

    JobAPI.Config conf = job.getConfig();
    LOG.log(Level.INFO, "Number of configs to override from job conf: " + conf.getKvsCount());

    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      builder.put(kv.getKey(), kv.getValue());
      LOG.log(Level.INFO, "Overriden conf key-value pair: " + kv.getKey() + ": " + kv.getValue());
    }

    return builder.build();
  }

  /**
   * loadConfig from config files
   * @return
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
   *
   * This method is copied from:
   * https://stackoverflow.com/questions/27187566/load-jar-dynamically-at-runtime
   * @param jarFile
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  public static boolean loadLibrary(String jarFile) {
    try {
      File jar = new File(jarFile);
      /*We are using reflection here to circumvent encapsulation; addURL is not public*/
      java.net.URLClassLoader loader = (java.net.URLClassLoader) ClassLoader.getSystemClassLoader();
      java.net.URL url = jar.toURI().toURL();
      /*Disallow if already loaded*/
      for (java.net.URL it: java.util.Arrays.asList(loader.getURLs())) {
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

    String flagFileName = POD_SHARED_VOLUME + "/" + UNPACK_COMPLETE_FILE_NAME;
    String completionsFileName = POD_SHARED_VOLUME + "/" + COMPLETIONS_FILE_NAME;

    // if it is the first container in a pod, unpack the tar.gz file
    if (containerName.endsWith("-0")) {

      boolean transferred = waitForFileTransfer(jobPackageFileName, fileSize);
      if (transferred) {
        boolean jobFileUnpacked = unpackJobPackage(jobPackageFileName);
        if (jobFileUnpacked) {
          System.out.printf("Job file [%s] unpacked successfully.\n", jobPackageFileName);
          boolean written1 = writeFile(flagFileName, 0);
          boolean written2 = writeFile(completionsFileName, 0);
          return written1 && written2;
        } else {
          System.out.println("Job file can not be unpacked.");
          return false;
        }
      } else {
        System.out.println("Something went wrong with receiving job file.");
        return false;
      }

    } else {
      return waitForFlagFile(flagFileName);
    }
  }

  /**
   * unpack the received job package
   * job package needs to be a tar.gz package
   * it unpacks to the directory where the job package resides
   * @param sourceGzip
   * @return
   */
  private static boolean unpackJobPackage(final String sourceGzip) {

    File sourceGzipFile = new File(sourceGzip);
    File outputDir = sourceGzipFile.getParentFile();

    GzipCompressorInputStream gzIn = null;
    TarArchiveInputStream tarInputStream = null;

    try {
      // construct input stream
      InputStream fin = Files.newInputStream(Paths.get(sourceGzip));
      BufferedInputStream in = new BufferedInputStream(fin);
      gzIn = new GzipCompressorInputStream(in);
      tarInputStream = new TarArchiveInputStream(gzIn);

      TarArchiveEntry entry = null;

      while ((entry = (TarArchiveEntry) tarInputStream.getNextEntry()) != null) {

        File outputFile = new File(outputDir, entry.getName());
        if (!outputFile.getParentFile().exists()) {
          boolean dirCreated = outputFile.getParentFile().mkdirs();
          if (!dirCreated) {
            LOG.severe("Can not create the output directory: " + outputFile.getParentFile()
                + "\nFile unpack is unsuccessful.");
            return false;
          }
        }

        if (!outputFile.isDirectory()) {
          final OutputStream outputFileStream = new FileOutputStream(outputFile);
          IOUtils.copy(tarInputStream, outputFileStream);
          outputFileStream.close();
//          LOG.info("Unpacked the file: " + outputFile.getAbsolutePath());
        }
      }

      tarInputStream.close();
      gzIn.close();
      return true;

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when unpacking job package. ", e);
      return false;
    }
  }

  /**
   * write a file with an int in it.
   * @return
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
      e.printStackTrace();
      LOG.severe("Exception when writing the file: " + fileName);
      return false;
    }
  }



  /**
   * Wait for the hob package file to be transferred to this pod
   * @param jobFileName
   * @param fileSize
   * @return
   */
  public static boolean waitForFileTransfer(String jobFileName, long fileSize) {

    boolean transferred = false;
    File jobFile = new File(jobFileName);

    // when waiting, it will print log message at least after this much time
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
        e.printStackTrace();
      }
    }

    return false;
  }

  /**
   * The workers except the one in the first container in a pod wait for
   * the first worker to write the unpack-complete.txt file
   * @param flagFileName
   * @return
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
        e.printStackTrace();
      }
    }

    return false;
  }
}
