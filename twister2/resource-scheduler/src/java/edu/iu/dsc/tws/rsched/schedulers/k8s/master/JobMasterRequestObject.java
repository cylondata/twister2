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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.K8sEnvVariables;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1EnvVarSource;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServicePort;
import io.kubernetes.client.models.V1ServiceSpec;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import io.kubernetes.client.models.V1beta2StatefulSet;
import io.kubernetes.client.models.V1beta2StatefulSetSpec;

public final class JobMasterRequestObject {
  private static final Logger LOG = Logger.getLogger(JobMasterRequestObject.class.getName());

  private static Config config;
  private static String jobName;
  private static String encodedNodeInfoList;

  private JobMasterRequestObject() { }

  public static void init(Config cnfg, String jName) {
    config = cnfg;
    jobName = jName;
  }

  /**
   * create StatefulSet object for a job
   * @return
   */
  public static V1beta2StatefulSet createStatefulSetObject(String nodeInfoListStr) {

    if (config == null) {
      LOG.severe("JobMasterRequestObject.init method has not been called.");
      return null;
    }
    encodedNodeInfoList = nodeInfoListStr;

    V1beta2StatefulSet statefulSet = new V1beta2StatefulSet();
    statefulSet.setApiVersion("apps/v1beta2");
    statefulSet.setKind("StatefulSet");

    // construct metadata and set for jobName setting
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(KubernetesUtils.createJobMasterStatefulSetName(jobName));
    statefulSet.setMetadata(meta);

    // construct JobSpec and set
    V1beta2StatefulSetSpec setSpec = new V1beta2StatefulSetSpec();
    setSpec.serviceName(KubernetesUtils.createJobMasterServiceName(jobName));
    setSpec.setReplicas(1);

    // add selector for the job
    V1LabelSelector selector = new V1LabelSelector();
    String jobMasterServiceLabel = KubernetesUtils.createJobMasterServiceLabel(jobName);
    selector.putMatchLabelsItem(KubernetesConstants.SERVICE_LABEL_KEY, jobMasterServiceLabel);
    setSpec.setSelector(selector);

    // construct the pod template
    V1PodTemplateSpec template = constructPodTemplate();
    setSpec.setTemplate(template);

    statefulSet.setSpec(setSpec);

    return statefulSet;
  }

  /**
   * construct pod template
   * @return
   */
  public static V1PodTemplateSpec constructPodTemplate() {

    V1PodTemplateSpec template = new V1PodTemplateSpec();
    V1ObjectMeta templateMetaData = new V1ObjectMeta();
    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put(KubernetesConstants.SERVICE_LABEL_KEY,
        KubernetesUtils.createJobMasterServiceLabel(jobName));

    String jobPodsLabel = KubernetesUtils.createJobPodsLabel(jobName);
    labels.put(KubernetesConstants.TWISTER2_JOB_PODS_KEY, jobPodsLabel);

    String jobMasterRoleLabel = KubernetesUtils.createJobMasterRoleLabel(jobName);
    labels.put(KubernetesConstants.TWISTER2_PODS_ROLE_KEY, jobMasterRoleLabel);

    templateMetaData.setLabels(labels);
    template.setMetadata(templateMetaData);

    V1PodSpec podSpec = new V1PodSpec();
    podSpec.setTerminationGracePeriodSeconds(0L);

    ArrayList<V1Volume> volumes = new ArrayList<>();

    // a volatile disk based volume
    // create it if the requested disk space is positive
    if (JobMasterContext.volatileVolumeRequested(config)) {
      double vSize = JobMasterContext.volatileVolumeSize(config);
      V1Volume volatileVolume = RequestObjectBuilder.createVolatileVolume(vSize);
      volumes.add(volatileVolume);
    }

    if (JobMasterContext.persistentVolumeRequested(config)) {
      String claimName = KubernetesUtils.createPersistentVolumeClaimName(jobName);
      V1Volume persistentVolume = RequestObjectBuilder.createPersistentVolume(claimName);
      volumes.add(persistentVolume);
    }

    V1Volume configMapVolume = createConfigMapVolume();
    volumes.add(configMapVolume);

    podSpec.setVolumes(volumes);

    ArrayList<V1Container> containers = new ArrayList<V1Container>();
    containers.add(constructContainer());
    podSpec.setContainers(containers);

    template.setSpec(podSpec);
    return template;
  }

  public static V1Volume createConfigMapVolume() {
    V1Volume configMapVolume = new V1Volume();
    configMapVolume.setName(KubernetesConstants.CONFIG_MAP_VOLUME_NAME);
    V1ConfigMapVolumeSource volumeSource = new V1ConfigMapVolumeSource();
    volumeSource.setName(KubernetesUtils.createJobMasterConfigMapName(jobName));
    configMapVolume.setConfigMap(volumeSource);
    return configMapVolume;
  }



  /**
   * construct a container
   * @return
   */
  public static V1Container constructContainer() {
    // construct container and add it to podSpec
    V1Container container = new V1Container();
    container.setName("twister2-job-master");

    String containerImage = KubernetesContext.twister2DockerImageForK8s(config);
    if (containerImage == null) {
      throw new RuntimeException("Container Image name is null. Config parameter: "
          + "twister2.docker.image.for.kubernetes can not be null");
    }
    container.setImage(containerImage);

    // by default: IfNotPresent
    // can be set to Always from client.yaml
    container.setImagePullPolicy(KubernetesContext.imagePullPolicy(config));

//        container.setArgs(Arrays.asList("1000000")); parameter to the main method
    container.setCommand(
        Arrays.asList("java", "edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterStarter"));

    V1ResourceRequirements resReq = new V1ResourceRequirements();
    resReq.putRequestsItem("cpu", new Quantity(JobMasterContext.jobMasterCpu(config) + ""));
    resReq.putRequestsItem("memory", new Quantity(JobMasterContext.jobMasterRAM(config) + "Mi"));
    container.setResources(resReq);

    ArrayList<V1VolumeMount> volumeMounts = new ArrayList<>();

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

    V1VolumeMount configMapVolumeMount = new V1VolumeMount();
    configMapVolumeMount.setName(KubernetesConstants.CONFIG_MAP_VOLUME_NAME);
    configMapVolumeMount.setMountPath(KubernetesConstants.CONFIG_MAP_VOLUME_MOUNT);
    volumeMounts.add(configMapVolumeMount);

    container.setVolumeMounts(volumeMounts);

    V1ContainerPort port = new V1ContainerPort();
    port.name("job-master-port");
    port.containerPort(JobMasterContext.jobMasterPort(config));
    port.setProtocol("TCP");
    container.setPorts(Arrays.asList(port));

    container.setEnv(constructEnvironmentVariables());

    return container;
  }

  /**
   * set environment variables for containers
   */
  public static List<V1EnvVar> constructEnvironmentVariables() {
    ArrayList<V1EnvVar> envVars = new ArrayList<>();

    envVars.add(new V1EnvVar()
        .name(K8sEnvVariables.JOB_NAME + "")
        .value(jobName));

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

    return envVars;
  }

  /**
   * create regular service for job master
   * @return
   */
  public static V1Service createJobMasterServiceObject() {

    String serviceName = KubernetesUtils.createJobMasterServiceName(jobName);
    String serviceLabel = KubernetesUtils.createJobMasterServiceLabel(jobName);

    V1Service service = new V1Service();
    service.setKind("Service");
    service.setApiVersion("v1");

    // construct and set metadata
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(serviceName);
    service.setMetadata(meta);

    // construct and set service spec
    V1ServiceSpec serviceSpec = new V1ServiceSpec();
    // set selector
    HashMap<String, String> selectors = new HashMap<String, String>();
    selectors.put(KubernetesConstants.SERVICE_LABEL_KEY, serviceLabel);
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
   * @return
   */
  public static V1Service createJobMasterHeadlessServiceObject() {

    String serviceName = KubernetesUtils.createJobMasterServiceName(jobName);
    String serviceLabel = KubernetesUtils.createJobMasterServiceLabel(jobName);

    V1Service service = new V1Service();
    service.setKind("Service");
    service.setApiVersion("v1");

    // construct and set metadata
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(serviceName);
    service.setMetadata(meta);

    // construct and set service spec
    V1ServiceSpec serviceSpec = new V1ServiceSpec();
    serviceSpec.setClusterIP("None");

    // set selector
    HashMap<String, String> selectors = new HashMap<String, String>();
    selectors.put(KubernetesConstants.SERVICE_LABEL_KEY, serviceLabel);
    serviceSpec.setSelector(selectors);

    service.setSpec(serviceSpec);

    return service;
  }

  /**
   * create a ConfigMap object for Job Master
   * It will have job as binary data and config files as text data
   * @param job
   * @return
   */
  public static V1ConfigMap createJobMasterConfigMap(JobAPI.Job job) {
    String configMapName = KubernetesUtils.createJobMasterConfigMapName(jobName);

    V1ConfigMap configMap = new V1ConfigMap();
    configMap.apiVersion("v1");
    configMap.setKind("ConfigMap");

    // construct and set metadata
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(configMapName);
    configMap.setMetadata(meta);

    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(job.getJobName());
    configMap.putBinaryDataItem(jobDescFileName, job.toByteArray());

    String clientYaml = Context.clientConfigurationFile(config);
    String fileContent = readYAMLFile(clientYaml);
    configMap.putDataItem("client.yaml", fileContent);

    String taskYaml = Context.taskConfigurationFile(config);
    fileContent = readYAMLFile(taskYaml);
    configMap.putDataItem("task.yaml", fileContent);

    String resourceYaml = Context.resourceSchedulerConfigurationFile(config);
    fileContent = readYAMLFile(resourceYaml);
    configMap.putDataItem("resource.yaml", fileContent);

    String uploaderYaml = Context.uploaderConfigurationFile(config);
    fileContent = readYAMLFile(uploaderYaml);
    configMap.putDataItem("uploader.yaml", fileContent);

    String networkYaml = Context.networkConfigurationFile(config);
    fileContent = readYAMLFile(networkYaml);
    configMap.putDataItem("network.yaml", fileContent);

    String systemYaml = Context.systemConfigurationFile(config);
    fileContent = readYAMLFile(systemYaml);
    configMap.putDataItem("system.yaml", fileContent);

    String dataYaml = Context.dataConfigurationFile(config);
    fileContent = readYAMLFile(dataYaml);
    configMap.putDataItem("data.yaml", fileContent);

    return configMap;
  }

  /**
   * read the given YAML file as a single String
   * @param filename
   * @return
   */
  private static String readYAMLFile(String filename) {

    Path filepath = Paths.get(filename);

    if (!Files.exists(filepath)) {
      LOG.fine("Config file " + filename + " does not exist. "
          + "It will not be transferred to JobMaster.");
      return null;
    }

    try {
      return new String(Files.readAllBytes(filepath));
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Can not read the config file: " + filename
          + " This config file will not be transferred to JobMaster.", e);
      return null;
    }
  }

}
