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
package edu.iu.dsc.tws.rsched.schedulers.k8s;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.resource.RequestedResources;
import edu.iu.dsc.tws.rsched.spi.scheduler.ILauncher;

import io.kubernetes.client.models.V1PersistentVolume;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1beta2StatefulSet;

public class KubernetesLauncher implements ILauncher {

  private static final Logger LOG = Logger.getLogger(KubernetesLauncher.class.getName());

  private Config config;
  private KubernetesController controller;
  private String namespace;

  public KubernetesLauncher() {
    controller = new KubernetesController();
  }

  @Override
  public void initialize(Config conf) {
    this.config = conf;
    namespace = KubernetesContext.namespace(config);
    controller.init();
  }

  /**
   * Launch the processes according to the resource plan.
   *
   * @param resourceRequest requested resources
   * @return true if the request is granted
   */
  @Override
  public boolean launch(RequestedResources resourceRequest, JobAPI.Job job) {

    if (!configParametersOK()) {
      return false;
    }

    String jobName = job.getJobName();

    String jobPackageFile = SchedulerContext.temporaryPackagesPath(config) + "/"
        + SchedulerContext.jobPackageFileName(config);

    File jobFile = new File(jobPackageFile);
    if (!jobFile.exists()) {
      LOG.log(Level.SEVERE, "Can not access job package file: " + jobPackageFile
          + "\n++++++ Aborting submission. ++++++");
      return false;
    }

    long jobFileSize = jobFile.length();

    // initialize the service in Kubernetes master
    initService(jobName);

    // if persistent volume is requested, create a persistent volume and a persistent volume claim
    if (SchedulerContext.persistentVolumeRequested(config)) {
      boolean volumesSetup = initPersistentVolumes(jobName);
      if (!volumesSetup) {
        LOG.log(Level.SEVERE, "Please run terminate job to clear up any artifacts "
            + "from previous jobs, before submitting a new job,"
            + "or submit the job with a different name."
            + "\n++++++ Aborting submission. ++++++");
        return false;
      }
    }

    // initialize a stateful set for this job
    boolean statefulSetInitialized = initStatefulSet(jobName, resourceRequest, jobFileSize);
    if (!statefulSetInitialized) {
      return false;
    }

    // start the Job Master locally
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      JobMaster jobMaster = null;
      try {
        jobMaster = new JobMaster(config, InetAddress.getLocalHost().getHostAddress());
      } catch (UnknownHostException e) {
        LOG.log(Level.SEVERE, "Exception when getting local host address: ", e);
      }
      jobMaster.init();
    }

    // transfer the job package to pods, measure the transfer time
    long start = System.currentTimeMillis();

    int containersPerPod = KubernetesContext.workersPerPod(config);
    int numberOfPods = resourceRequest.getNoOfContainers() / containersPerPod;

    boolean transferred;
    if (KubernetesContext.persistentVolumeRequested(config)
        && KubernetesContext.persistentVolumeUploading(config)) {
      transferred = controller.transferJobPackage(namespace, jobName, jobPackageFile);
    } else {
      transferred =
          controller.transferJobPackageInParallel(namespace, jobName, numberOfPods, jobPackageFile);
    }

    if (transferred) {
      long duration = System.currentTimeMillis() - start;
      LOG.info("Transferring all files took: " + duration + " ms.");
    } else {
      LOG.log(Level.SEVERE, "Transferring the job package to some pods failed."
          + "\nPlease run terminate job to clear up any artifacts from previous jobs, "
          + "or submit the job with a different name."
          + "\n++++++ Aborting submission. ++++++");
//      terminateJob(jobName);
      return false;
    }

    return true;
  }

  private void initService(String jobName) {
    // first check whether there is a running service with the same name
    String serviceName = KubernetesUtils.createServiceName(jobName);
    V1Service service = controller.getService(namespace, serviceName);
    if (service != null) {
      LOG.log(Level.WARNING, "There is already a service with the name: " + serviceName
          + "\nAnother job might be running. "
          + "\nFirst terminate that job or create a job with a different name."
          + "\n++++++ Aborting submission ++++++");
      throw new RuntimeException();
    }

    // if NodePort service is requested start one,
    // otherwise start a headless service
    if (KubernetesContext.nodePortServiceRequested(config)) {

      if (KubernetesContext.workersPerPod(config) != 1) {
        LOG.log(Level.SEVERE, KubernetesContext.WORKERS_PER_POD + " value must be 1, "
            + "when starting NodePort service. Please change the config value and resubmit the job"
            + "\n++++++ Aborting submission ++++++");
        throw new RuntimeException();
      }

      service = RequestObjectBuilder.createNodePortServiceObject(config, jobName);
    } else {
      service = RequestObjectBuilder.createHeadlessServiceObject(config, jobName);
    }

    boolean serviceCreated = controller.createService(namespace, service);
    if (!serviceCreated) {
      LOG.log(Level.SEVERE, "Service could not be created."
          + "\n++++++ Aborting submission ++++++");
      throw new RuntimeException();
    }
  }

  private boolean initPersistentVolumes(String jobName) {

    // first check whether there is already a persistent volume with the same name
    // if not, create a new one
    String pvName = KubernetesUtils.createPersistentVolumeName(jobName);
    V1PersistentVolume pv = controller.getPersistentVolume(pvName);
    if (pv == null) {
      pv = RequestObjectBuilder.createPersistentVolumeObject(config, pvName);
      boolean pvCreated = controller.createPersistentVolume(pv);
      if (!pvCreated) {
        LOG.log(Level.SEVERE, "PersistentVolume could not be created. "
            + "\n++++++ Aborting submission ++++++");
        throw new RuntimeException();
      }
    } else {
      LOG.log(Level.SEVERE, "There is already a PersistentVolume with the name: " + pvName
          + "\nPlease terminate any artifacts from previous jobs or change your job name. "
          + "\n++++++ Aborting submission ++++++");
      return false;
    }

    String pvcName = KubernetesUtils.createStorageClaimName(jobName);
    // check whether there is a PersistentVolumeClaim object, if so, no need to create a new one
    // otherwise create a PersistentVolumeClaim
    V1PersistentVolumeClaim pvc = controller.getPersistentVolumeClaim(namespace, pvcName);
    if (pvc == null) {
      pvc = RequestObjectBuilder.createPersistentVolumeClaimObject(config, pvcName);
      boolean claimCreated = controller.createPersistentVolumeClaim(namespace, pvc);
      if (!claimCreated) {
        LOG.log(Level.SEVERE, "PersistentVolumeClaim could not be created. "
            + "\n++++++ Aborting submission ++++++");
        throw new RuntimeException();
      }
    } else {
      LOG.log(Level.WARNING, "There is already a PersistentVolumeClaim with the name: " + pvcName
          + "\nPlease terminate any artifacts from previous jobs or change your job name. "
          + "\n++++++ Aborting submission ++++++");
      return false;
    }

    return true;
  }


  private boolean initStatefulSet(String jobName, RequestedResources resourceRequest,
                                  long jobFileSize) {

    // first check whether there is a StatefulSet with the same name,
    // if so, do not submit new job. Give a message and terminate
    // user needs to explicitly terminate that job
    String serviceLabelWithKey = KubernetesUtils.createServiceLabelWithKey(jobName);
    V1beta2StatefulSet existingStatefulSet =
        controller.getStatefulSet(namespace, jobName, serviceLabelWithKey);
    if (existingStatefulSet != null) {
      LOG.log(Level.SEVERE, "There is already a StatefulSet object in Kubernetes master "
          + "with the name: " + jobName + "\nFirst terminate this running job and resubmit. "
          + "\n++++++ Aborting submission ++++++");
      return false;
    }

    // create the StatefulSet object for this job
    V1beta2StatefulSet statefulSet = RequestObjectBuilder.createStatefulSetObjectForJob(
        jobName, resourceRequest, jobFileSize, config);

    if (statefulSet == null) {
      return false;
    }

    boolean statefulSetCreated = controller.createStatefulSetJob(namespace, statefulSet);
    if (!statefulSetCreated) {
      LOG.log(Level.SEVERE, "\nPlease run terminate job to clear up any artifacts from "
          + "previous jobs."
          + "\n++++++ Aborting submission ++++++");
      return false;
    }

    return true;
  }

  private boolean configParametersOK() {

    // if statically binding requested, number for CPUs per worker has to be an integer
    if (KubernetesContext.bindWorkerToCPU(config)) {
      double cpus = SchedulerContext.workerCPU(config);
      if (cpus % 1 != 0) {
        LOG.log(Level.SEVERE, String.format("When %s is true, the value of %s has to be an int"
            + "\n%s= " + cpus
            + "\n++++++ Aborting submission ++++++",
            KubernetesContext.K8S_BIND_WORKER_TO_CPU, SchedulerContext.TWISTER2_WORKER_CPU,
            SchedulerContext.TWISTER2_WORKER_CPU));
        return false;
      }
    }

    // number of workers has to be divisible by the workersPerPod
    // all pods will have equal number of containers
    // all pods will be identical
    int containersPerPod = KubernetesContext.workersPerPod(config);
    int numberOfWorkers = SchedulerContext.workerInstances(config);
    if (numberOfWorkers % containersPerPod != 0) {
      LOG.log(Level.SEVERE, String.format("%s has to be divisible by %s."
          + "\n%s: " + numberOfWorkers
          + "\n%s: " + containersPerPod
          + "\n++++++ Aborting submission ++++++",
          SchedulerContext.TWISTER2_WORKER_INSTANCES, KubernetesContext.WORKERS_PER_POD,
          SchedulerContext.TWISTER2_WORKER_INSTANCES, KubernetesContext.WORKERS_PER_POD));
      return false;
    }

    // when worker to nodes mapping is requested
    // if the operator is Exists or DoesNotExist,
    // values list must be empty
    if (KubernetesContext.workerToNodeMapping(config)) {
      String operator = KubernetesContext.workerMappingOperator(config);
      List<String> values = KubernetesContext.workerMappingValues(config);
      if (("Exists".equalsIgnoreCase(operator) || "DoesNotExist".equalsIgnoreCase(operator))
          && values != null && values.size() != 0) {
        LOG.log(Level.SEVERE, String.format("When the value of %s is either Exists or DoesNotExist"
                + "\n%s list must be empty. Current content of this list: " + values
                + "\n++++++ Aborting submission ++++++",
            KubernetesContext.K8S_WORKER_MAPPING_OPERATOR,
            KubernetesContext.K8S_WORKER_MAPPING_VALUES));
        return false;
      }
    }

    return true;
  }

  /**
   * Close up any resources
   */
  @Override
  public void close() {
  }

  /**
   * Terminate the Kubernetes Job
   */
  @Override
  public boolean terminateJob(String jobName) {

    // first delete the StatefulSet
    boolean statefulSetDeleted = controller.deleteStatefulSetJob(namespace, jobName);

    // delete the service
    String serviceName = KubernetesUtils.createServiceName(jobName);
    boolean deleted = controller.deleteService(namespace, serviceName);

    // delete the persistent volume claim
    String pvcName = KubernetesUtils.createStorageClaimName(jobName);
    boolean claimDeleted = controller.deletePersistentVolumeClaim(namespace, pvcName);

    // delete the persistent volume
    String pvName = KubernetesUtils.createPersistentVolumeName(jobName);
    boolean pvDeleted = controller.deletePersistentVolume(pvName);

    return true;
  }
}
