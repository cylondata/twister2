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

package edu.iu.dsc.tws.rsched.schedulers.k8s.master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.K8sEnvVariables;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EmptyDirVolumeSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

public final class JobMasterRequestObject {
  private static final Logger LOG = Logger.getLogger(JobMasterRequestObject.class.getName());

  private static Config config;
  private static String jobID;
  private static String encodedNodeInfoList;
  private static long jobPackageFileSize;

  private JobMasterRequestObject() {
  }

  public static void init(Config cnfg, String jID, long jpFileSize) {
    config = cnfg;
    jobID = jID;
    jobPackageFileSize = jpFileSize;
  }

  /**
   * create StatefulSet object for a job
   */
  public static V1StatefulSet createStatefulSetObject(String nodeInfoListStr) {

    if (config == null) {
      LOG.severe("JobMasterRequestObject.init method has not been called.");
      return null;
    }
    encodedNodeInfoList = nodeInfoListStr;

    V1StatefulSet statefulSet = new V1StatefulSet();
    String statefulSetName = KubernetesUtils.createJobMasterStatefulSetName(jobID);

    // set labels for the jm stateful set
    HashMap<String, String> labels = new HashMap<>();
    labels.put("app", "twister2");
    labels.put("t2-job", jobID);
    labels.put("t2-mss", jobID); // job master statefulset

    // construct metadata and set for jobID setting
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(statefulSetName);
    meta.setLabels(labels);
    statefulSet.setMetadata(meta);

    // construct JobSpec and set
    V1StatefulSetSpec setSpec = new V1StatefulSetSpec();
    setSpec.serviceName(KubernetesUtils.createJobMasterServiceName(jobID));
    setSpec.setReplicas(1);

    // add selector for the job
    V1LabelSelector selector = new V1LabelSelector();
    selector.putMatchLabelsItem("t2-mp", jobID);
    setSpec.setSelector(selector);

    // construct the pod template
    V1PodTemplateSpec template = constructPodTemplate();
    setSpec.setTemplate(template);

    statefulSet.setSpec(setSpec);

    return statefulSet;
  }

  /**
   * construct pod template
   */
  public static V1PodTemplateSpec constructPodTemplate() {

    V1PodTemplateSpec template = new V1PodTemplateSpec();
    V1ObjectMeta templateMetaData = new V1ObjectMeta();
    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put("app", "twister2");
    labels.put("t2-job", jobID);
    labels.put("t2-mp", jobID); // job master pod

    templateMetaData.setLabels(labels);
    template.setMetadata(templateMetaData);

    V1PodSpec podSpec = new V1PodSpec();
    podSpec.setTerminationGracePeriodSeconds(0L);

    ArrayList<V1Volume> volumes = new ArrayList<>();
    V1Volume memoryVolume = new V1Volume();
    memoryVolume.setName(KubernetesConstants.POD_MEMORY_VOLUME_NAME);
    V1EmptyDirVolumeSource volumeSource1 = new V1EmptyDirVolumeSource();
    volumeSource1.setMedium("Memory");
    memoryVolume.setEmptyDir(volumeSource1);
    volumes.add(memoryVolume);

    // a volatile disk based volume
    // create it if the requested disk space is positive
    if (JobMasterContext.volatileVolumeRequested(config)) {
      double vSize = JobMasterContext.volatileVolumeSize(config);
      V1Volume volatileVolume = RequestObjectBuilder.createVolatileVolume(vSize);
      volumes.add(volatileVolume);
    }

    if (JobMasterContext.persistentVolumeRequested(config)) {
      String claimName = KubernetesUtils.createPersistentVolumeClaimName(jobID);
      V1Volume persistentVolume = RequestObjectBuilder.createPersistentVolume(claimName);
      volumes.add(persistentVolume);
    }

    podSpec.setVolumes(volumes);

    ArrayList<V1Container> containers = new ArrayList<V1Container>();
    containers.add(constructContainer());
    podSpec.setContainers(containers);

    template.setSpec(podSpec);
    return template;
  }

  /**
   * construct a container
   */
  public static V1Container constructContainer() {
    // construct container and add it to podSpec
    V1Container container = new V1Container();
    container.setName("twister2-job-master-0");

    String containerImage = KubernetesContext.twister2DockerImageForK8s(config);
    if (containerImage == null) {
      throw new RuntimeException("Container Image name is null. Config parameter: "
          + "twister2.resource.kubernetes.docker.image can not be null");
    }
    container.setImage(containerImage);
    container.setImagePullPolicy(KubernetesContext.imagePullPolicy(config));
    container.setCommand(Arrays.asList("/bin/bash"));
    container.setArgs(Arrays.asList("-c", "./init.sh"));

    int jmRam = JobMasterContext.jobMasterRAM(config) + 128;
    V1ResourceRequirements resReq = new V1ResourceRequirements();
    resReq.putRequestsItem("cpu", new Quantity(JobMasterContext.jobMasterCpu(config) + ""));
    resReq.putRequestsItem("memory", new Quantity(jmRam + "Mi"));
    container.setResources(resReq);

    ArrayList<V1VolumeMount> volumeMounts = new ArrayList<>();
    V1VolumeMount memoryVolumeMount = new V1VolumeMount();
    memoryVolumeMount.setName(KubernetesConstants.POD_MEMORY_VOLUME_NAME);
    memoryVolumeMount.setMountPath(KubernetesConstants.POD_MEMORY_VOLUME);
    volumeMounts.add(memoryVolumeMount);

    if (JobMasterContext.volatileVolumeRequested(config)) {
      V1VolumeMount volatileVolumeMount = new V1VolumeMount();
      volatileVolumeMount.setName(KubernetesConstants.POD_VOLATILE_VOLUME_NAME);
      volatileVolumeMount.setMountPath(KubernetesConstants.POD_VOLATILE_VOLUME);
      volumeMounts.add(volatileVolumeMount);
    }

    if (JobMasterContext.persistentVolumeRequested(config)) {
      V1VolumeMount persVolumeMount = new V1VolumeMount();
      persVolumeMount.setName(KubernetesConstants.PERSISTENT_VOLUME_NAME);
      persVolumeMount.setMountPath(KubernetesConstants.PERSISTENT_VOLUME_MOUNT);
      volumeMounts.add(persVolumeMount);
    }

    container.setVolumeMounts(volumeMounts);

    V1ContainerPort port = new V1ContainerPort();
    port.name("job-master-port");
    port.containerPort(JobMasterContext.jobMasterPort(config));
    port.setProtocol("TCP");
    container.setPorts(Arrays.asList(port));

    container.setEnv(constructEnvironmentVariables(JobMasterContext.jobMasterRAM(config)));

    return container;
  }

  /**
   * set environment variables for containers
   */
  public static List<V1EnvVar> constructEnvironmentVariables(int jvmMem) {
    ArrayList<V1EnvVar> envVars = new ArrayList<>();

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.JOB_ID + "")
        .value(jobID));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.ENCODED_NODE_INFO_LIST + "")
        .value(encodedNodeInfoList));

    // HOST_IP (node-ip) with downward API
    V1ObjectFieldSelector fieldSelector = new V1ObjectFieldSelector();
    fieldSelector.setFieldPath("status.hostIP");
    V1EnvVarSource varSource = new V1EnvVarSource();
    varSource.setFieldRef(fieldSelector);

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.HOST_IP + "")
        .valueFrom(varSource));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.JOB_PACKAGE_FILE_SIZE + "")
        .value(jobPackageFileSize + ""));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.CONTAINER_NAME + "")
        .value("twister2-job-master-0"));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.USER_JOB_JAR_FILE + "")
        .value(SchedulerContext.userJobJarFile(config)));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.CLASS_TO_RUN + "")
        .value("edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterStarter"));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.POD_MEMORY_VOLUME + "")
        .value(KubernetesConstants.POD_MEMORY_VOLUME));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.JOB_ARCHIVE_DIRECTORY + "")
        .value(Context.JOB_ARCHIVE_DIRECTORY));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.JOB_PACKAGE_FILENAME + "")
        .value(JobUtils.createJobPackageFileName(jobID)));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.UPLOAD_METHOD + "")
        .value(RequestObjectBuilder.uploadMethod));

    String uri = null;
    if (SchedulerContext.jobPackageUri(config) != null) {
      uri = SchedulerContext.jobPackageUri(config).toString();
    }

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.JOB_PACKAGE_URI + "")
        .value(uri));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.LOGGER_PROPERTIES_FILE + "")
        .value(LoggingContext.LOGGER_PROPERTIES_FILE));

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.JVM_MEMORY_MB + "")
        .value(jvmMem + ""));

    return envVars;
  }

  /**
   * create regular service for job master
   */
  public static V1Service createJobMasterServiceObject() {

    String serviceName = KubernetesUtils.createJobMasterServiceName(jobID);

    V1Service service = new V1Service();
    service.setKind("Service");
    service.setApiVersion("v1");

    // set labels for the jm service
    HashMap<String, String> labels = new HashMap<>();
    labels.put("app", "twister2");
    labels.put("t2-job", jobID);

    // construct and set metadata
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(serviceName);
    meta.setLabels(labels);
    service.setMetadata(meta);

    // construct and set service spec
    V1ServiceSpec serviceSpec = new V1ServiceSpec();
    // set selector
    HashMap<String, String> selectors = new HashMap<String, String>();
    selectors.put("t2-mp", jobID);
    serviceSpec.setSelector(selectors);
    // set port
    V1ServicePort servicePort = new V1ServicePort();
    servicePort.setName("job-master-port");
    servicePort.setPort(JobMasterContext.jobMasterPort(config));
    servicePort.setTargetPort(new IntOrString(JobMasterContext.jobMasterPort(config)));
    servicePort.setProtocol("TCP");
    serviceSpec.setPorts(Arrays.asList(servicePort));

    service.setSpec(serviceSpec);

    return service;
  }

  /**
   * create headless service for job master
   */
  public static V1Service createJobMasterHeadlessServiceObject() {

    String serviceName = KubernetesUtils.createJobMasterServiceName(jobID);

    V1Service service = new V1Service();
    service.setKind("Service");
    service.setApiVersion("v1");

    // set labels for the jm service
    HashMap<String, String> labels = new HashMap<>();
    labels.put("app", "twister2");
    labels.put("t2-job", jobID);

    // construct and set metadata
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(serviceName);
    meta.setLabels(labels);
    service.setMetadata(meta);

    // construct and set service spec
    V1ServiceSpec serviceSpec = new V1ServiceSpec();
    serviceSpec.setClusterIP("None");

    // set selector
    HashMap<String, String> selectors = new HashMap<String, String>();
    selectors.put("t2-mp", jobID);
    serviceSpec.setSelector(selectors);

    service.setSpec(serviceSpec);

    return service;
  }

}
