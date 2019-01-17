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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.NodeInfoUtils;
import edu.iu.dsc.tws.master.IJobTerminator;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.interfaces.ILauncher;
import edu.iu.dsc.tws.rsched.schedulers.k8s.driver.K8sScaler;
import edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterRequestObject;
import edu.iu.dsc.tws.rsched.schedulers.k8s.uploader.UploaderForJob;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1beta2StatefulSet;

public class KubernetesLauncher implements ILauncher, IJobTerminator {

  private static final Logger LOG = Logger.getLogger(KubernetesLauncher.class.getName());

  private Config config;
  private KubernetesController controller;
  private String namespace;
  private JobSubmissionStatus jobSubmissionStatus;
  private UploaderForJob uploader;

  public KubernetesLauncher() {
    controller = new KubernetesController();
    jobSubmissionStatus = new JobSubmissionStatus();
  }

  @Override
  public void initialize(Config conf) {
    this.config = conf;
    namespace = KubernetesContext.namespace(config);
    controller.init(namespace);
  }

  /**
   * Launch the processes according to the resource plan.
   *
   * @return true if the request is granted
   */
  @Override
  public boolean launch(JobAPI.Job job) {

    if (!configParametersOK(job)) {
      return false;
    }


    String jobName = job.getJobName();

    String jobPackageFile = SchedulerContext.temporaryPackagesPath(config) + "/"
        + SchedulerContext.jobPackageFileName(config);

    File jobFile = new File(jobPackageFile);
    if (!jobFile.exists()) {
      LOG.log(Level.SEVERE, "Can not access job package file: " + jobPackageFile
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    long jobFileSize = jobFile.length();

    RequestObjectBuilder.init(config, job.getJobName(), jobFileSize);
    JobMasterRequestObject.init(config, job.getJobName(), jobFileSize);

    // check all relevant entities on Kubernetes master
    boolean allEntitiesOK = checkEntitiesOnKubernetesMaster(job);
    if (!allEntitiesOK) {
      return false;
    }

    // start job package transfer threads to watch pods to starting
    if (KubernetesContext.clientToPodsUploading(config)) {
      uploader = new UploaderForJob(config, job, jobPackageFile);
      uploader.start();
    }

    // initialize the service in Kubernetes master
    boolean servicesCreated = initServices(jobName);
    if (!servicesCreated) {
      clearupWhenSubmissionFails(jobName);
      return false;
    }

    // if persistent volume is requested, create a persistent volume claim
    if (SchedulerContext.persistentVolumeRequested(config)) {
      boolean volumesSetup = initPersistentVolumeClaim(job);
      if (!volumesSetup) {
        clearupWhenSubmissionFails(jobName);
        return false;
      }
    }

    // initialize StatefulSets for this job
    boolean statefulSetInitialized = initStatefulSets(job);
    if (!statefulSetInitialized) {
      clearupWhenSubmissionFails(jobName);
      return false;
    }

    if (KubernetesContext.clientToPodsUploading(config)) {
      // transfer the job package to pods, measure the transfer time
      long start = System.currentTimeMillis();
      boolean transferred = uploader.completeFileTransfers();

      if (transferred) {
        long duration = System.currentTimeMillis() - start;
        LOG.info("Transferring all files took: " + duration + " ms.");
      } else {
        LOG.log(Level.SEVERE, "Transferring the job package to some pods failed."
            + "\nPlease run terminate job to clear up any artifacts from previous jobs, "
            + "or submit the job with a different name."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");

        clearupWhenSubmissionFails(jobName);
        return false;
      }
    }

    // start the Job Master locally if requested
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      boolean jobMasterCompleted = startJobMasterOnClient(job);
      if (!jobMasterCompleted) {
        LOG.log(Level.SEVERE, "JobMaster can not be started. "
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        clearupWhenSubmissionFails(jobName);
        return false;
      }
    }

    return true;
  }

  private String getJobMasterIP(JobAPI.Job job) {
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      try {
        return InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        throw new RuntimeException("Exception when getting local host address: ", e);
      }
    }

    String jobMasterIP = PodWatchUtils.getJobMasterIpByWatchingPodToRunning(
        KubernetesContext.namespace(config), job.getJobName(), 100);

    if (jobMasterIP == null) {
      throw new RuntimeException("Job master is running in a separate pod, but "
          + "this worker can not get the job master IP address from Kubernetes master.\n"
          + "Job master address: " + jobMasterIP);
    }

    LOG.info("Job master address: " + jobMasterIP);
    return jobMasterIP;
  }

  /**
   * start the JobMaster locally on submitting client
   * this is a blocking call
   * it finishes after the job has completed
   */
  private boolean startJobMasterOnClient(JobAPI.Job job) {

    String hostAdress = null;
    try {
      hostAdress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Exception when getting local host address: ", e);
      return false;
    }

    JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo(hostAdress, null, null);
    K8sScaler k8sScaler = new K8sScaler(config, job, controller);
    JobMaster jobMaster = new JobMaster(config, hostAdress, this, job, nodeInfo, k8sScaler);
    jobMaster.addShutdownHook(true);
//    jobMaster.startJobMasterThreaded();
    jobMaster.startJobMasterBlocking();

    return true;
  }

  /**
   * check whether there are any entities with the same name on Kubernetes master
   * check the existence of all entities that will be created for this job
   * if any one of them exist, return false,
   * otherwise return true
   * <p>
   * for OpenMPI enabled jobs, check whether the Secret object exist on Kubernetes master
   * if it does not exist, return false
   */
  private boolean checkEntitiesOnKubernetesMaster(JobAPI.Job job) {

    String jobName = job.getJobName();

    // first check the existence of services that will be created for this job
    // if any one of them exists, return false
    String workersServiceName = KubernetesUtils.createServiceName(job.getJobName());
    ArrayList<String> serviceNames = new ArrayList<>();
    serviceNames.add(workersServiceName);

    String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobName);
    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      serviceNames.add(jobMasterServiceName);
    }

    boolean serviceExists = controller.existServices(serviceNames);
    if (serviceExists) {
      LOG.severe("Another job might be running. "
          + "\nFirst terminate that job or create a job with a different name."
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    // if persistent volume is requested,
    // check whether a PersistentVolumeClaim with the same name exist
    if (SchedulerContext.persistentVolumeRequested(config)) {
      String pvcName = KubernetesUtils.createPersistentVolumeClaimName(jobName);
      if (controller.existPersistentVolumeClaim(pvcName)) {
        LOG.severe("Another job might be running. "
            + "\nFirst terminate that job or create a job with a different name."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    // check whether there is already a StatefulSet with the same name,
    ArrayList<String> statefulSetNames = new ArrayList<>();
    for (int i = 0; i < job.getComputeResourceList().size(); i++) {
      statefulSetNames.add(KubernetesUtils.createWorkersStatefulSetName(jobName, i));
    }

    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      String jobMasterStatefulSetName = KubernetesUtils.createJobMasterStatefulSetName(jobName);
      statefulSetNames.add(jobMasterStatefulSetName);
    }

    boolean statefulSetExists = controller.existStatefulSets(statefulSetNames);

    if (statefulSetExists) {
      LOG.severe("First terminate the previously running job with the same name. "
          + "\nOr submit the job with a different job name"
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    // check the existence of Secret object on Kubernetes master
    // when OpenMPI is enabled, a Secret object has to be available in the cluster
    if (SchedulerContext.useOpenMPI(config)) {
      String secretName = KubernetesContext.secretName(config);
      boolean secretExists = controller.existSecret(secretName);

      if (!secretExists) {
        LOG.severe("No Secret object is available in the cluster with the name: " + secretName
            + "\nFirst create this object or make that object created by your cluster admin."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    return true;
  }

  private boolean initServices(String jobName) {

    String workersServiceName = KubernetesUtils.createServiceName(jobName);
    String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobName);

    // if NodePort service is requested start one,
    // otherwise start a headless service
    V1Service serviceForWorkers = null;
    if (KubernetesContext.nodePortServiceRequested(config)) {
      serviceForWorkers = RequestObjectBuilder.createNodePortServiceObject();
    } else {
      serviceForWorkers = RequestObjectBuilder.createJobServiceObject();
    }

    boolean serviceCreated = controller.createService(serviceForWorkers);
    if (serviceCreated) {
      jobSubmissionStatus.setServiceForWorkersCreated(true);
    } else {
      LOG.severe("Following service could not be created: " + workersServiceName
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    // if Job Master runs as a separate pod, initialize a service for that
    if (!JobMasterContext.jobMasterRunsInClient(config)) {

      V1Service serviceForJobMaster = JobMasterRequestObject.createJobMasterHeadlessServiceObject();
//      V1Service serviceForJobMaster = JobMasterRequestObject.createJobMasterServiceObject();
      serviceCreated = controller.createService(serviceForJobMaster);
      if (serviceCreated) {
        jobSubmissionStatus.setServiceForJobMasterCreated(true);
      } else {
        LOG.severe("Following service could not be created: " + jobMasterServiceName
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    return true;
  }

  /**
   * initialize a PersistentVolumeClaim on Kubernetes master
   */
  private boolean initPersistentVolumeClaim(JobAPI.Job job) {

    String pvcName = KubernetesUtils.createPersistentVolumeClaimName(job.getJobName());
    V1PersistentVolumeClaim pvc =
        RequestObjectBuilder.createPersistentVolumeClaimObject(pvcName, job.getNumberOfWorkers());

    boolean claimCreated = controller.createPersistentVolumeClaim(pvc);
    if (claimCreated) {
      jobSubmissionStatus.setPvcCreated(true);
    } else {
      LOG.log(Level.SEVERE, "PersistentVolumeClaim could not be created. "
          + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
      return false;
    }

    return true;
  }

  private boolean initStatefulSets(JobAPI.Job job) {

    String encodedNodeInfoList = null;
    // if node locations will be retrieved from Kubernetes master
    if (!KubernetesContext.nodeLocationsFromConfig(config)) {
      // first get Node list and build encoded NodeInfoUtils Strings
      String rackLabelKey = KubernetesContext.rackLabelKeyForK8s(config);
      String dcLabelKey = KubernetesContext.datacenterLabelKeyForK8s(config);
      ArrayList<JobMasterAPI.NodeInfo> nodeInfoList =
          controller.getNodeInfo(rackLabelKey, dcLabelKey);
      encodedNodeInfoList = NodeInfoUtils.encodeNodeInfoList(nodeInfoList);
      LOG.fine("NodeInfo objects: size " + nodeInfoList.size()
          + "\n" + NodeInfoUtils.listToString(nodeInfoList));
    }

    // create StatefulSet for the job master
    if (!JobMasterContext.jobMasterRunsInClient(config)) {

      // create the StatefulSet object for this job
      V1beta2StatefulSet jobMasterStatefulSet =
          JobMasterRequestObject.createStatefulSetObject(encodedNodeInfoList);
      if (jobMasterStatefulSet == null) {
        return false;
      }

      boolean statefulSetCreated = controller.createStatefulSet(jobMasterStatefulSet);
      if (statefulSetCreated) {
        jobSubmissionStatus.addCreatedStatefulSetName(jobMasterStatefulSet.getMetadata().getName());
      } else {
        LOG.severe("Please run terminate job to clear up any artifacts from previous jobs."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    // let the transfer threads know that we are about to submit the StatefulSets
    // JobPackageTransferThread.setSubmittingStatefulSets();

    // create StatefulSets for workers
    for (int i = 0; i < job.getComputeResourceList().size(); i++) {

      JobAPI.ComputeResource computeResource = JobUtils.getComputeResource(job, i);
      if (computeResource == null) {
        LOG.severe("Something wrong with the job object. Can not get ComputeResource from job"
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }

      // create the StatefulSet object for this job
      V1beta2StatefulSet statefulSet = RequestObjectBuilder.createStatefulSetForWorkers(
          computeResource, encodedNodeInfoList);

      if (statefulSet == null) {
        return false;
      }

      boolean statefulSetCreated = controller.createStatefulSet(statefulSet);
      if (statefulSetCreated) {
        jobSubmissionStatus.addCreatedStatefulSetName(statefulSet.getMetadata().getName());
      } else {
        LOG.severe("Please run terminate job to clear up any artifacts from previous jobs."
            + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
        return false;
      }
    }

    return true;
  }

  /**
   * check whether configuration parameters are accurate
   */
  private boolean configParametersOK(JobAPI.Job job) {

    // when OpenMPI is enabled, all pods need to have equal numbers workers
    // we check whether all workersPerPod values are equal to first ComputeResource workersPerPod
    if (SchedulerContext.useOpenMPI(config)) {
      int workersPerPod = job.getComputeResource(0).getWorkersPerPod();
      for (int i = 1; i < job.getComputeResourceList().size(); i++) {
        if (workersPerPod != job.getComputeResource(i).getWorkersPerPod()) {
          LOG.log(Level.SEVERE, String.format("When OpenMPI is used, all workersPerPod values "
              + "in ComputeResources have to be the same. "
              + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++"));
          return false;
        }
      }
    }

    // if statically binding requested, number for CPUs per worker has to be an integer
    if (KubernetesContext.bindWorkerToCPU(config)) {
      for (JobAPI.ComputeResource computeResource : job.getComputeResourceList()) {
        double cpus = computeResource.getCpu();
        if (cpus % 1 != 0) {
          LOG.log(Level.SEVERE, String.format("When %s is true, the value of cpu has to be an int"
                  + " cpu= " + cpus
                  + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++",
              KubernetesContext.K8S_BIND_WORKER_TO_CPU));
          return false;
        }
      }
    }

    // when worker to nodes mapping is requested
    // if the operator is either Exists or DoesNotExist,
    // values list must be empty
    if (KubernetesContext.workerToNodeMapping(config)) {
      String operator = KubernetesContext.workerMappingOperator(config);
      List<String> values = KubernetesContext.workerMappingValues(config);
      if (("Exists".equalsIgnoreCase(operator) || "DoesNotExist".equalsIgnoreCase(operator))
          && values != null && values.size() != 0) {
        LOG.log(Level.SEVERE, String.format("When the value of %s is either Exists or DoesNotExist"
                + "\n%s list must be empty. Current content of this list: " + values
                + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++",
            KubernetesContext.K8S_WORKER_MAPPING_OPERATOR,
            KubernetesContext.K8S_WORKER_MAPPING_VALUES));
        return false;
      }
    }

    // When nodePort service requested, WORKERS_PER_POD value must be 1
    if (KubernetesContext.nodePortServiceRequested(config)) {
      for (JobAPI.ComputeResource computeResource : job.getComputeResourceList()) {
        if (computeResource.getWorkersPerPod() != 1) {
          LOG.log(Level.SEVERE, "workersPerPod value must be 1, when starting NodePort service. "
              + "Please change the config value and resubmit the job"
              + "\n++++++++++++++++++ Aborting submission ++++++++++++++++++");
          return false;
        }
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
   * We clear up the resources that we created during the job submission process
   * we don't delete the resources that may be left from previous jobs
   * They have to be deleted explicitly by the user
   */
  private void clearupWhenSubmissionFails(String jobName) {

    LOG.info("Will clear up any resources created during the job submission process.");

    if (KubernetesContext.clientToPodsUploading(config) && uploader != null) {
      uploader.stopUploader();
    }

    // first delete the service objects
    // delete the job service
    if (jobSubmissionStatus.isServiceForWorkersCreated()) {
      String serviceName = KubernetesUtils.createServiceName(jobName);
      controller.deleteService(serviceName);
    }

    // delete the job master service
    if (jobSubmissionStatus.isServiceForJobMasterCreated()) {
      String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobName);
      controller.deleteService(jobMasterServiceName);
    }

    // delete created StatefulSet objects
    ArrayList<String> ssNameLists = jobSubmissionStatus.getCreatedStatefulSetNames();
    for (String ssName : ssNameLists) {
      controller.deleteStatefulSet(ssName);
    }

    // delete the persistent volume claim
    if (jobSubmissionStatus.isPvcCreated()) {
      String pvcName = KubernetesUtils.createPersistentVolumeClaimName(jobName);
      boolean claimDeleted = controller.deletePersistentVolumeClaim(pvcName);
    }
  }

  /**
   * Terminate the Kubernetes Job
   */
  @Override
  public boolean terminateJob(String jobName) {

    // first delete the job master StatefulSet
    String jobMasterStatefulSetName = KubernetesUtils.createJobMasterStatefulSetName(jobName);
    boolean deleted = controller.deleteStatefulSet(jobMasterStatefulSetName);

    // delete workers the StatefulSets
    ArrayList<String> ssNameLists = controller.getStatefulSetsForJobWorkers(jobName);
    for (String ssName : ssNameLists) {
      controller.deleteStatefulSet(ssName);
    }

    // delete the job service
    String serviceName = KubernetesUtils.createServiceName(jobName);
    deleted = controller.deleteService(serviceName);

    // delete the job master service
    String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobName);
    controller.deleteService(jobMasterServiceName);

    // delete the persistent volume claim
    String pvcName = KubernetesUtils.createPersistentVolumeClaimName(jobName);
    boolean claimDeleted = controller.deletePersistentVolumeClaim(pvcName);

    return true;
  }
}
