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
package edu.iu.dsc.tws.api;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
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
   * @param twister2Job job
   */
  public static void submitJob(Twister2Job twister2Job, Config config) {

    // if this is a Kubernetes cluster, check the job name,
    // if it does not conform to Kubernetes rules, change it
    if (Context.clusterType(config).equals(KubernetesConstants.KUBERNETES_CLUSTER_TYPE)) {
      processJobNameForK8s(twister2Job);
    }

    // save the job to transfer to workers
    JobAPI.Job job = twister2Job.serialize();

    // update the config object with the values from job
    Config updatedConfig = JobUtils.updateConfigs(job, config);

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
   * @return
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
