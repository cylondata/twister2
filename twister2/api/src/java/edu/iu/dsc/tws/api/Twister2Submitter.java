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
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
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

    Twister2Job updatedTwister2Job = updateTwister2Job(twister2Job, config);

    // if this is a Kubernetes cluster, check the job name,
    // if it does not conform to Kubernetes rules, change it
    if (Context.clusterType(config).equals(KubernetesConstants.KUBERNETES_CLUSTER_TYPE)) {
      updatedTwister2Job = processJobNameForK8s(updatedTwister2Job);
    }

    // save the job to transfer to workers
    JobAPI.Job job = updatedTwister2Job.serialize();

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

        LOG.info("JobName does not conform to Kubernetes naming rules: " + jobName
            + "\nOnly lower case alphanumeric characters, dash(-), and dot(.) are allowed");

        jobName = KubernetesUtils.convertJobNameToK8sFormat(jobName);

        LOG.info("********* JobName modified to: " + jobName + " This name will be used *********");
      }
    }

    // launch the launcher
    ResourceAllocator resourceAllocator = new ResourceAllocator();
    resourceAllocator.terminateJob(jobName, config);
  }

  /**
   * update any missing value of Twister2Job object from configs
   * @return
   */
  public static Twister2Job updateTwister2Job(Twister2Job twister2Job, Config config) {

    // check whether all fields are set, if so, return
    if (twister2Job.getName() != null
        && twister2Job.getWorkerClass() != null
        && twister2Job.getNumberOfWorkers() != 0
        && twister2Job.getRequestedResource() != null) {
      return twister2Job;
    }

    Twister2Job.BasicJobBuilder builder = Twister2Job.newBuilder();

    String jobName = twister2Job.getName();
    if (jobName == null) {
      if (Context.jobName(config) == null) {
        throw new RuntimeException("Job name is not set.");
      } else {
        jobName = Context.jobName(config);
        LOG.info("JobName is read from config file: " + jobName);
      }
    }
    builder.setName(jobName);

    String workerClass = twister2Job.getWorkerClass();
    if (workerClass == null) {
      if (SchedulerContext.workerClass(config) == null) {
        throw new RuntimeException("Worker class name is not set.");
      } else {
        workerClass = SchedulerContext.workerClass(config);
      }
    }
    builder.setWorkerClass(workerClass);

    int workerInstances = twister2Job.getNumberOfWorkers();
    if (workerInstances == 0) {
      if (Context.workerInstances(config) == 0) {
        throw new RuntimeException("Number of Worker Instances is not set.");
      } else {
        workerInstances = Context.workerInstances(config);
      }
    }

    WorkerComputeResource computeResource = twister2Job.getRequestedResource();
    if (computeResource == null) {
      double cpuPerWorker = Context.workerCPU(config);
      int ramPerWorker = Context.workerRAM(config);
      double diskPerWorker = Context.workerVolatileDisk(config);
      if (cpuPerWorker == 0) {
        throw new RuntimeException("CPU per worker is not set.");
      } else if (ramPerWorker == 0) {
        throw new RuntimeException("RAM per worker is not set.");
      } else {
        computeResource = new WorkerComputeResource(cpuPerWorker, ramPerWorker, diskPerWorker);
      }
    }
    builder.setRequestResource(computeResource, workerInstances);

    builder.setConfig(twister2Job.getConfig());

    return builder.build();
  }

  /**
   * write the values from Job object to config object
   * only write the values that are initialized
   * @return
   */
  public static Twister2Job processJobNameForK8s(Twister2Job twister2Job) {

    String jobName = twister2Job.getName();

    // if it is a proper job name, return the job object
    if (KubernetesUtils.jobNameConformsToK8sNamingRules(jobName)) {
      return twister2Job;
    }

    LOG.info("JobName does not conform to Kubernetes naming rules: " + jobName
        + "\nOnly lower case alphanumeric characters, dash(-), and dot(.) are allowed");

    jobName = KubernetesUtils.convertJobNameToK8sFormat(jobName);

    LOG.info("********* JobName modified to: " + jobName + " This name will be used *********");

    return Twister2Job.newBuilder()
        .setName(jobName)
        .setWorkerClass(twister2Job.getWorkerClass())
        .setRequestResource(twister2Job.getRequestedResource(), twister2Job.getNumberOfWorkers())
        .setConfig(twister2Job.getConfig())
        .build();
  }

}
