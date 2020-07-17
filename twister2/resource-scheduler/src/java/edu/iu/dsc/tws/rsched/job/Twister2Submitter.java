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

package edu.iu.dsc.tws.rsched.job;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.uploaders.localfs.FsContext;
import edu.iu.dsc.tws.rsched.utils.FileUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.TarGzipPacker;

public final class Twister2Submitter {
  private static final Logger LOG = Logger.getLogger(Twister2Submitter.class.getName());

  private Twister2Submitter() {
  }

  public static void submitJob(Twister2Job twister2Job) {
    submitJob(twister2Job, ResourceAllocator.loadConfig(twister2Job.getConfig()));
  }

  /**
   * Submit a Twister2 job
   *
   * @param twister2Job job
   */
  public static Twister2JobState submitJob(Twister2Job twister2Job, Config config) {

    // set jobID if it is set in configuration file,
    // otherwise it will be automatically generated
    if (Context.jobId(config) != null) {
      twister2Job.setJobID(Context.jobId(config));
    }

    // set username
    String userName = Context.userName(config);
    if (userName == null) {
      userName = System.getProperty("user.name");
    }
    twister2Job.setUserName(userName);

    JobAPI.Job job = twister2Job.serialize();

    // update the config object with the values from job
    Config updatedConfig = JobUtils.updateConfigs(job, config);

    writeJobIdToFile(job.getJobId());
    printJobInfo(job, updatedConfig);

    // launch the launcher
    ResourceAllocator resourceAllocator = new ResourceAllocator(updatedConfig, job);
    return resourceAllocator.submitJob();
  }

  /**
   * Restart a Twister2 job
   */
  public static Twister2JobState restartJob(String jobID, Config config) {

    // job package filename from failed submission
    String prevJobDir = FsContext.uploaderJobDirectory(config) + File.separator + jobID;
    String jobPackage = prevJobDir + File.separator + SchedulerContext.jobPackageFileName(config);

    Path jobPackageFile = Paths.get(jobPackage);
    if (Files.notExists(jobPackageFile)) {
      LOG.severe("Job Package File does not exist: " + jobPackage);
      return new Twister2JobState(false);
    }

    // unpack the previous job package to a temp directory
    Path tempDirPath;
    try {
      tempDirPath = Files.createTempDirectory(jobID);
    } catch (IOException e) {
      throw new Twister2RuntimeException("Can not create temp directory", e);
    }
    //todo: we can exclude user-job-file from being unpacked
    //      usually that is the lastest file, so we can be more efficient
    TarGzipPacker.unpack(jobPackageFile, tempDirPath);

    // load Job object
    String unpackedJobDir = tempDirPath + File.separator + Context.JOB_ARCHIVE_DIRECTORY;
    String jobFile = unpackedJobDir
        + File.separator + SchedulerContext.createJobDescriptionFileName(jobID);
    JobAPI.Job job = JobUtils.readJobFile(jobFile);

    // load previous configurations
    Config prevConfig = ConfigLoader.loadConfig(Context.twister2Home(config),
        unpackedJobDir, Context.clusterType(config));

    // delete temp directory
    try {
      Files.delete(tempDirPath);
      LOG.info("Unpacked job directory deleted: " + tempDirPath);
    } catch (IOException e) {
      LOG.warning("Exception when deleting temp directory: " + tempDirPath);
    }

    URI packageURI = null;
    try {
      packageURI = new URI(prevJobDir);
    } catch (URISyntaxException e) {
      throw new Twister2RuntimeException("Can not ceate URI for directory: " + prevJobDir, e);
    }

    // add restore parameter
    // local packages path
    prevConfig = Config.newBuilder().putAll(prevConfig)
        .put(CheckpointingContext.CHECKPOINTING_RESTORE_JOB, true)
        .put(SchedulerContext.TEMPORARY_PACKAGES_PATH, prevJobDir)
        .put(SchedulerContext.USER_JOB_FILE, job.getJobFormat().getJobFile())
        .put(SchedulerContext.JOB_PACKAGE_URI, packageURI)
        .put(Context.TWISTER2_HOME.getKey(), Context.twister2Home(config))
        .put(Context.JOB_ID, jobID)
        .put(Context.TWISTER2_CLUSTER_TYPE, Context.clusterType(config))
        .build();

    writeJobIdToFile(jobID);
    printJobInfo(job, prevConfig);

    // launch the launcher
    ResourceAllocator resourceAllocator = new ResourceAllocator(prevConfig, job);
    return resourceAllocator.resubmitJob();
  }

  private static void writeJobIdToFile(String jobID) {
    // write jobID to file
    String dir = System.getProperty("user.home") + File.separator + ".twister2";
    if (!FileUtils.isDirectoryExists(dir)) {
      FileUtils.createDirectory(dir);
    }
    String filename = dir + File.separator + "last-job-id.txt";
    FileUtils.writeToFile(filename, (jobID + "").getBytes(), true);
  }

  private static void printJobInfo(JobAPI.Job job, Config config) {
    //print ascii
    LOG.info("\n\n _____           _     _           ____  \n"
        + "/__   \\__      _(_)___| |_ ___ _ _|___ \\ \n"
        + "  / /\\/\\ \\ /\\ / / / __| __/ _ \\ '__|__) |\n"
        + " / /    \\ V  V /| \\__ \\ ||  __/ |  / __/ \n"
        + " \\/      \\_/\\_/ |_|___/\\__\\___|_| |_____| v0.8.0-SNAPSHOT\n"
        + "                                         \n"
        + "Job ID      \t  :\t" + job.getJobId() + "\n"
        + "Number of Workers :\t" + job.getNumberOfWorkers() + "\n"
        + "Worker Class\t  :\t" + job.getWorkerClassName() + "\n"
        + JobUtils.computeResourcesToPrint(job) + "\n"
        + "Cluster Type\t  :\t" + Context.clusterType(config) + "\n"
        + "Runtime     \t  :\t" + System.getProperty("java.vm.name")
        + " " + System.getProperty("java.vm.version") + "\n"
        + "\n"
    );
  }

  /**
   * terminate a Twister2 job
   */
  public static void terminateJob(String jobID, Config config) {
    ResourceAllocator.killJob(jobID, config);
  }

  public static void clearJob(String jobID, Config config) {

    deleteJobDir(jobID, config);

    // close the controller
    if (KubernetesContext.isKubernetesCluster(config)) {
      KubernetesController controller =
          KubernetesController.init(KubernetesContext.namespace(config));
      controller.deletePersistentVolumeClaim(jobID);
      KubernetesController.close();
    }

    //todo: need to delete checkpointed data in other clusters and on HDFS
  }

  /**
   * Delete the directory that has the Job package for the checkpointed job
   */
  private static void deleteJobDir(String jobID, Config config) {
    // job package directory
    String jobDir = FsContext.uploaderJobDirectory(config) + File.separator + jobID;
    Path jobPackageFile = Paths.get(jobDir);
    if (Files.notExists(jobPackageFile)) {
      LOG.severe("Job Package directory does not exist: " + jobDir);
    } else {
      LOG.info("Cleaning job directory: " + jobDir);
      FileUtils.deleteDir(jobDir);
    }
  }

  /**
   * Clear left over resources for all checkpointed jobs that are not currently running:
   *   The Job package for the checkpointed job stored in user home directory
   *   Checkpointed data that are saved when jobs failed.
   */
  public static void clearAllJobs(Config config) {

    // get the list of currently running jobs
    List<String> runningJobs = new LinkedList<>();

    KubernetesController controller = null;
    if (KubernetesContext.isKubernetesCluster(config)) {
      controller = KubernetesController.init(KubernetesContext.namespace(config));
      runningJobs = controller.getTwister2ConfigMapNames();
    }
    //todo: get running jobs from other clusters

    // get all local job directories
    List<String> localJobDirs = FileUtils.getDirectories(FsContext.uploaderJobDirectory(config));
    // remove running jobs
    localJobDirs.removeIf(runningJobs::contains);
    // remove local directories
    localJobDirs.forEach(jd -> deleteJobDir(jd, config));

    // delete PVC for the jobs that have ended in Kubernetes
    if (KubernetesContext.isKubernetesCluster(config)) {
      List<String> pvcList = controller.getTwister2PersistentVolumeClaims();
      pvcList.removeIf(runningJobs::contains);
      pvcList.forEach(controller::deletePersistentVolumeClaim);
      controller.close();
    }

    //todo: need to delete checkpointed data in other clusters and on HDFS
  }


}
