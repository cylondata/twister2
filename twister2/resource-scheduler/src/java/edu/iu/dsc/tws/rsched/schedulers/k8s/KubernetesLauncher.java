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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
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

    String jobName = job.getJobName();

    String jobPackageFile = SchedulerContext.temporaryPackagesPath(config) + "/"
        + SchedulerContext.jobPackageFileName(config);

    File jobFile = new File(jobPackageFile);
    if (!jobFile.exists()) {
      LOG.log(Level.SEVERE, "Can not access job package file: " + jobPackageFile
          + "\nAborting submission.");
      return false;
    }

    long jobFileSize = jobFile.length();

    // initialize the service in Kubernetes master
    initService(jobName);

    // if persistent volume is requested, create a persistent volume and a persistent volume claim
    if (SchedulerContext.persistentVolumeRequested(config)) {
      boolean volumesSetup = initPersistentVolumes(jobName);
      if (!volumesSetup) {
        LOG.log(Level.SEVERE, "Aborting submission."
            + "\nPlease run terminate job to clear up any artifacts from this job, "
            + "before submitting a new job.");
        return false;
      }
    }

    // initialize a stateful set for this job
    boolean statefulSetInitialized = initStatefulSet(jobName, resourceRequest, jobFileSize);
    if (!statefulSetInitialized) {
      return false;
    }

    // transfer the job package to pods, measure the transfer time
    long start = System.currentTimeMillis();

    int containersPerPod = KubernetesContext.containersPerPod(config);
    int numberOfPods = resourceRequest.getNoOfContainers() / containersPerPod;

    boolean transferred =
        controller.transferJobPackageInParallel(namespace, jobName, numberOfPods, jobPackageFile);

    if (transferred) {
      long duration = System.currentTimeMillis() - start;
      LOG.info("Transferring all files took: " + duration + " ms.");
    } else {
      LOG.log(Level.SEVERE, "Transferring the job package to some pods failed. Aborting submission."
          + "\nPlease run terminate job to clear up any artifacts from this job, "
          + "before submitting a new job.");
//      terminateJob(jobName);
      return false;
    }

    return true;
  }

  private void initService(String jobName) {
    // first check whether there is a running service
    String serviceName = KubernetesUtils.createServiceName(jobName);
    String serviceLabel = KubernetesUtils.createServiceLabel(jobName);
    V1Service service = controller.getService(namespace, serviceName);

    // if there is no service, start one
    if (service == null) {
      int port = KubernetesContext.servicePort(config);
      int targetPort = KubernetesContext.serviceTargetPort(config);
      service = KubernetesUtils.createServiceObject(serviceName, serviceLabel, port, targetPort);
      boolean serviceCreated = controller.createService(namespace, service);
      if (!serviceCreated) {
        LOG.log(Level.SEVERE, "Service could not be created. Aborting submission");
        throw new RuntimeException();
      }

      // if there is already a service with the same name
    } else {
      LOG.log(Level.WARNING, "There is already a service with the name: " + serviceName
          + "\nNo need to create a new service. Will use the existing one.");
    }
  }


  private boolean initPersistentVolumes(String jobName) {

    // first check whether there is already a persistent volume with the same name
    // if not, create a new one
    String pvName = KubernetesUtils.createPersistentVolumeName(jobName);
    V1PersistentVolume pv = controller.getPersistentVolume(pvName);
    if (pv == null) {
      pv = KubernetesUtils.createPersistentVolumeObject(config, pvName);
      boolean pvCreated = controller.createPersistentVolume(pv);
      if (!pvCreated) {
        LOG.log(Level.SEVERE, "PersistentVolume could not be created. Aborting submission.");
        System.out.println("submitted pv: \n" + pv);
        throw new RuntimeException();
      }
    } else {
      LOG.log(Level.SEVERE, "There is already a PersistentVolume with the name: " + pvName
          + "\nPlease terminate any artifacts from previous jobs or change your job name. "
          + "Aborting submission.");
      return false;
    }

    String pvcName = KubernetesUtils.createStorageClaimName(jobName);
    // check whether there is a PersistentVolumeClaim object, if so, no need to create a new one
    // otherwise create a PersistentVolumeClaim
    V1PersistentVolumeClaim pvc = controller.getPersistentVolumeClaim(namespace, pvcName);
    if (pvc == null) {
      pvc = KubernetesUtils.createPersistentVolumeClaimObject(config, pvcName);
      boolean claimCreated = controller.createPersistentVolumeClaim(namespace, pvc);
      if (!claimCreated) {
        LOG.log(Level.SEVERE, "PersistentVolumeClaim could not be created. Aborting submission");
        throw new RuntimeException();
      }
    } else {
      LOG.log(Level.WARNING, "There is already a PersistentVolumeClaim with the name: " + pvcName
          + "\nPlease terminate any artifacts from previous jobs or change your job name. "
          + "Aborting submission.");
      return false;
    }

    return true;
  }


  private boolean initStatefulSet(String jobName, RequestedResources resourceRequest,
                                  long jobFileSize) {

    // first check whether there is a StatefulSet with the same name,
    // if so, do not submit new job. Give a message and terminate
    // user needs to explicitly terminate that job
    String serviceLabelWithApp = KubernetesUtils.createServiceLabelWithApp(jobName);
    V1beta2StatefulSet existingStatefulSet =
        controller.getStatefulSet(namespace, jobName, serviceLabelWithApp);
    if (existingStatefulSet != null) {
      LOG.log(Level.SEVERE, "There is already a StatefulSet object in Kubernetes master "
          + "with the name: " + jobName + "\nFirst terminate this running job and resubmit. ");
      return false;
    }

    // create the StatefulSet for this job
    V1beta2StatefulSet statefulSet = KubernetesUtils.createStatefulSetObjectForJob(
        jobName, resourceRequest, jobFileSize, config);

    if (statefulSet == null) {
      LOG.log(Level.SEVERE, "Aborting submission."
          + "\nPlease run terminate job to clear up any artifacts from this job, "
          + "before submitting a new job.");

      return false;
    }

    boolean statefulSetCreated = controller.createStatefulSetJob(namespace, statefulSet);
    if (!statefulSetCreated) {
      LOG.log(Level.SEVERE, "Aborting submission."
          + "\nPlease run terminate job to clear up any artifacts from this job, "
          + "before submitting a new job.");
      return false;
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
