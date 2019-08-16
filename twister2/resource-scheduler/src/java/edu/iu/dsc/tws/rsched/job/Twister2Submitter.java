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

import java.io.IOException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.ConfigSerializer;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class Twister2Submitter {
  private static final Logger LOG = Logger.getLogger(Twister2Submitter.class.getName());

  private Twister2Submitter() {
  }

  /**
   * Submit a Twister2 job
   *
   * @param twister2Job job
   */
  public static void submitJob(Twister2Job twister2Job, Config config) {
    // if this is a Kubernetes cluster, check the job name,
    // if it does not conform to Kubernetes rules, change it
    boolean startingFromACheckpoint = CheckpointingConfigurations.startingFromACheckpoint(config);
    if (!startingFromACheckpoint) {
      switch (Context.clusterType(config)) {
        case KubernetesConstants.KUBERNETES_CLUSTER_TYPE:
          processJobNameForK8s(twister2Job);
          break;
        case "mesos":
        case "nomad":
          twister2Job.setJobName(twister2Job.getJobName() + System.currentTimeMillis());
          break;
        default:
          //do nothing
      }
    }

    Config configCopy = JobUtils.resolveJobId(config, twister2Job.getJobName());

    // save the job to transfer to workers
    JobAPI.Job job = twister2Job.serialize();

    // get existing id or create a new job id
    String jobId = configCopy.getStringValue(Context.JOB_ID);

    //if checkpointing is enabled, twister2Job and config will be saved to the state backend
    if (CheckpointingConfigurations.isCheckpointingEnabled(configCopy)) {
      LOG.info("Checkpointing has enabled for this job.");

      StateStore stateStore = CheckpointUtils.getStateStore(configCopy);
      stateStore.init(configCopy, jobId);

      try {
        if (startingFromACheckpoint) {
          // if job is starting from a checkpoint and previous state is not found in store
          if (!CheckpointUtils.containsJobInStore(jobId, stateStore)) {
            throw new RuntimeException("Couldn't find job state in store to restart " + jobId);
          }

          //restarting the job
          LOG.info("Found job " + jobId + " in state store. Restoring...");
          byte[] jobMetaBytes = CheckpointUtils.restoreJobMeta(jobId, stateStore);
          job = JobAPI.Job.parseFrom(jobMetaBytes);

          byte[] configBytes = CheckpointUtils.restoreJobConfig(jobId, stateStore);
          configCopy = ConfigLoader.loadConfig(configBytes);
        } else {
          // first time running or re-running the job, backup configs
          LOG.info("Saving job config and metadata");
          CheckpointUtils.saveJobConfigAndMeta(
              jobId, job.toByteArray(), ConfigSerializer.serialize(configCopy), stateStore);
        }
      } catch (IOException e) {
        LOG.severe("Failed to submit th checkpointing enabled job");
        throw new RuntimeException(e);
      }
    }


    LOG.info("The job to be submitted: \n" + JobUtils.toString(job));

    // update the config object with the values from job
    Config updatedConfig = JobUtils.updateConfigs(job, configCopy);

    // launch the luancher
    ResourceAllocator resourceAllocator = new ResourceAllocator();
    resourceAllocator.submitJob(job, updatedConfig);
  }

  /**
   * terminate a Twister2 job
   */
  @SuppressWarnings("ParameterAssignment")
  public static void terminateJob(String jobName, Config config) {

    // if this is a Kubernetes cluster, check the job name,
    // if it does not conform to Kubernetes rules, change it
    if (Context.clusterType(config).equals(KubernetesConstants.KUBERNETES_CLUSTER_TYPE)) {
      if (!KubernetesUtils.jobNameConformsToK8sNamingRules(jobName)) {

        LOG.info("JobName does not conform to Kubernetes naming rules: [" + jobName
            + "] Only lower case alphanumeric characters and dash(-) are allowed.");

        jobName = KubernetesUtils.convertJobNameToK8sFormat(jobName);

        LOG.info("****************** JobName modified. Following jobname will be used: " + jobName);
      }
    }

    // launch the launcher
    ResourceAllocator resourceAllocator = new ResourceAllocator();
    resourceAllocator.terminateJob(jobName, config);
  }

  /**
   * write the values from Job object to config object
   * only write the values that are initialized
   */
  public static void processJobNameForK8s(Twister2Job twister2Job) {

    String jobName = twister2Job.getJobName();

    // if it is a proper job name, return
    if (KubernetesUtils.jobNameConformsToK8sNamingRules(jobName)) {
      return;
    }

    LOG.info("JobName does not conform to Kubernetes naming rules: " + jobName
        + " Only lower case alphanumeric characters and dashes(-) are allowed");

    jobName = KubernetesUtils.convertJobNameToK8sFormat(jobName);
    twister2Job.setJobName(jobName);

    LOG.info("******************* JobName modified. Following jobname will be used: " + jobName);
  }

}
