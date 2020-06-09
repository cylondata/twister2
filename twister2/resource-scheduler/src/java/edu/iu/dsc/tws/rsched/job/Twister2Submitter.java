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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.ConfigSerializer;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
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

    boolean startingFromACheckpoint = CheckpointingContext.startingFromACheckpoint(config);
    if (!startingFromACheckpoint) {
      switch (Context.clusterType(config)) {
        case KubernetesConstants.KUBERNETES_CLUSTER_TYPE:
          break;
        case "mesos":
        case "nomad":
          twister2Job.setJobName(twister2Job.getJobName() + System.currentTimeMillis());
          break;
        default:
          //do nothing
      }
    }

    // set jobID if it is set in configuration file,
    // otherwise it will be automatically generated
    twister2Job.setJobID(Context.jobId(config));

    // set username
    String userName = Context.userName(config);
    if (userName == null) {
      userName = System.getProperty("user.name");
    }
    twister2Job.setUserName(userName);

    JobAPI.Job job = twister2Job.serialize();
    LOG.info("The job to be submitted: \n" + JobUtils.toString(job));

    // update the config object with the values from job
    Config updatedConfig = JobUtils.updateConfigs(job, config);
    String jobId = job.getJobId();

    writeJobIdToFile(jobId);
    printJobInfo(job, updatedConfig);

    //if checkpointing is enabled, twister2Job and config will be saved to the state backend
    if (CheckpointingContext.isCheckpointingEnabled(updatedConfig)
        && !Context.isKubernetesCluster(updatedConfig)) {
      LOG.info("Checkpointing has been enabled for this job.");

      StateStore stateStore = CheckpointUtils.getStateStore(updatedConfig);
      stateStore.init(updatedConfig, job.getJobId());

      try {
        // first time running or re-running the job, backup configs
        LOG.info("Saving job object and config");
        CheckpointUtils.saveJobConfigAndMeta(
            jobId, job.toByteArray(), ConfigSerializer.serialize(updatedConfig), stateStore);
      } catch (IOException e) {
        LOG.severe("Failed to submit th checkpointing enabled job");
        throw new RuntimeException(e);
      }
    }

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

    // add restore parameter
    // local packages path
    prevConfig = Config.newBuilder().putAll(prevConfig)
        .put(CheckpointingContext.CHECKPOINTING_RESTORE_JOB, true)
        .put(SchedulerContext.TEMPORARY_PACKAGES_PATH, prevJobDir)
        .put(SchedulerContext.USER_JOB_FILE, job.getJobFormat().getJobFile())
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
        + " \\/      \\_/\\_/ |_|___/\\__\\___|_| |_____| v0.6.0-SNAPSHOT\n"
        + "                                         \n"
        + "Job Name\t:\t" + job.getJobName() + "\n"
        + "Job ID\t\t:\t" + job.getJobId() + "\n"
        + "# of Workers\t:\t" + job.getNumberOfWorkers() + "\n"
        + "Worker Class\t:\t" + job.getWorkerClassName() + "\n"
        + "Cluster Type\t:\t" + Context.clusterType(config) + "\n"
        + "Runtime\t\t:\t" + System.getProperty("java.vm.name")
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

}
