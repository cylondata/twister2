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
package edu.iu.dsc.tws.rsched.schedulers.mesos;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;


public final class MesosContext extends SchedulerContext {
  public static final String SCHEDULER_WORKING_DIRECTORY =
      "twister2.scheduler.mesos.scheduler.working.directory";

  public static final String MESOS_CLUSTER_NAME = "twister2.resource.scheduler.mesos.cluster";

  public static final String MESOS_MASTER_URI = "twister2.mesos.master.uri";

  public static final String MESOS_MASTER_HOST = "twister2.mesos.master.host";

  public static final String MESOS_NATIVE_LIBRARY_PATH = "twister2.mesos.native.library.path";

  public static final String MESOS_FETCH_URI = "twister2.mesos.fetch.uri";

  public static final String MESOS_FRAMEWORK_STAGING_TIMEOUT_MS =
      "twister2.mesos.framework.staging.timeout.ms";
  public static final String MESOS_FRAMEWORK_NAME =
      "twister2.mesos.framework.name";

  public static final String MESOS_SCHEDULER_DRIVER_STOP_TIMEOUT_MS =
      "twister2.mesos.scheduler.driver.stop.timeout.ms";

  public static final String MESOS_ROLE = "twister2.mesos.role";
  public static final String MESOS_TOPOLOGY_NAME = "twister2.mesos.topology.name";

  public static final String TWISTER2_CORE_PACKAGE_URI = "twister2.system.core-package.uri";
  public static final String TWISTER2_JOB_URI = "twister2.system.job.uri";

  public static final String ROLE = "twister2.resource.scheduler.mesos.role";
  public static final String ENVIRONMENT = "twister2.resource.scheduler.mesos.env";

  public static final String CPUS_PER_CONTAINER = "twister2.cpu_per_container";
  public static final String RAM_PER_CONTAINER = "twister2.ram_per_container";
  public static final String DISK_PER_CONTAINER = "twister2.disk_per_container";
  public static final String NUMBER_OF_CONTAINERS = "twister2.number_of_containers";
  public static final String CONTAINER_PER_WORKER = "twister2.container_per_worker";
  public static final String WORKER_PORT = "twister2.worker_port";
  public static final String DESIRED_NODES = "twister2.desired_nodes";
  public static final String USE_DOCKER_CONTAINER = "twister2.use_docker_container";
  public static final String MESOS_OVERLAY_NETWORK_NAME = "twister2.mesos.overlay.network.name";
  public static final String DOCKER_IMAGE_NAME = "twister2.docker.image.name";
  public static final String MESOS_WORKER_CLASS = "twister2.class.mesos.worker";
  public static final String MESOS_CONTAINER_CLASS = "twister2.job.worker.class";

  public static final int DEFAULT_RAM_SIZE = 128; // 1GB
  public static final int DEFAULT_DISK_SIZE = 128; // 1GB
  public static final int DEFAULT_CPU_AMOUNT = 1;
  public static final int DEFAULT_NUMBER_OF_CONTAINERS = 1;
  public static final int DEFAULT_CONTAINER_PER_WORKER = 1;
  public static final int DEFAULT_WORKER_PORT = 31000;
  public static final String DEFAULT_DESIRED_NODE = "all";
  public static final String DEFAULT_USE_DOCKER_CONTAINER = "false";

  private MesosContext() {
  }


  public static String mesosClusterName(Config cfg) {
    return cfg.getStringValue(MESOS_CLUSTER_NAME);
  }

  public static String getDesiredNodes(Config cfg) {
    return cfg.getStringValue(DESIRED_NODES, DEFAULT_DESIRED_NODE);
  }

  public static String getUseDockerContainer(Config cfg) {
    return cfg.getStringValue(USE_DOCKER_CONTAINER, DEFAULT_USE_DOCKER_CONTAINER);
  }

  public static String getMesosOverlayNetworkName(Config cfg) {
    return cfg.getStringValue(MESOS_OVERLAY_NETWORK_NAME);
  }

  public static String getDockerImageName(Config cfg) {
    return cfg.getStringValue(DOCKER_IMAGE_NAME);
  }

  public static String role(Config cfg) {
    return cfg.getStringValue(ROLE);
  }

  public static String environment(Config cfg) {
    return cfg.getStringValue(ENVIRONMENT);
  }

  public static int cpusPerContainer(Config cfg) {
    return cfg.getIntegerValue(CPUS_PER_CONTAINER, DEFAULT_CPU_AMOUNT);
  }

  public static int ramPerContainer(Config cfg) {
    return cfg.getIntegerValue(RAM_PER_CONTAINER, DEFAULT_RAM_SIZE);
  }

  public static int diskPerContainer(Config cfg) {
    return cfg.getIntegerValue(DISK_PER_CONTAINER, DEFAULT_DISK_SIZE);
  }

  public static int getWorkerPort(Config cfg) {
    return cfg.getIntegerValue(WORKER_PORT, DEFAULT_WORKER_PORT);
  }

  public static int numberOfContainers(Config cfg) {
    return cfg.getIntegerValue(NUMBER_OF_CONTAINERS, DEFAULT_NUMBER_OF_CONTAINERS);
  }
  public static int containerPerWorker(Config cfg) {
    return cfg.getIntegerValue(CONTAINER_PER_WORKER, DEFAULT_CONTAINER_PER_WORKER);
  }

  public static String MesosFrameworkName(Config cfg) {
    return cfg.getStringValue(MESOS_FRAMEWORK_NAME);
  }

  public static String mesosWorkerClass(Config cfg) {
    return cfg.getStringValue(MESOS_WORKER_CLASS);
  }
  public static String mesosContainerClass(Config cfg) {
    return cfg.getStringValue(MESOS_CONTAINER_CLASS);
  }
  public static  String MesosFetchURI(Config  cfg) {
    return cfg.getStringValue(MESOS_FETCH_URI);
  }

  public static  String jobURI(Config  cfg) {
    return cfg.getStringValue(TWISTER2_JOB_URI);
  }

  public static String getMesosMasterUri(Config config) {
    return config.getStringValue(MESOS_MASTER_URI);
  }

  public static String getMesosMasterHost(Config config) {
    return config.getStringValue(MESOS_MASTER_HOST);
  }
  /*
  public static String getSchedulerWorkingDirectory(Config config) {
    return config.getStringValue(SCHEDULER_WORKING_DIRECTORY);
  }

  public static String getMesosClusterName(Config config) {
    return config.getStringValue(SCHEDULER_WORKING_DIRECTORY);
  }

  public static String getMesosNativeLibraryPath(Config config) {
    return config.getStringValue(MESOS_NATIVE_LIBRARY_PATH);
  }

  public static long getMesosFrameworkStagingTimeoutMs(Config config) {
    return config.getLongValue(MESOS_FRAMEWORK_STAGING_TIMEOUT_MS, 1000);
  }

  public static long getMesosSchedulerDriverStopTimeoutMs(Config config) {
    return config.getLongValue(MESOS_SCHEDULER_DRIVER_STOP_TIMEOUT_MS, 1000);
  }

  public static String getMesosRole(Config config) {
    return config.getStringValue(MESOS_ROLE, "*");
  }

  public static String getMesosTopologyName(Config config) {
    return config.getStringValue(MESOS_TOPOLOGY_NAME, "to be determined.");
  }
  */
}
