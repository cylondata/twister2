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
package edu.iu.dsc.tws.common.config;

import java.util.HashMap;
import java.util.Map;

public class Context {
  // the entries used for configurations
  protected static Map<String, ConfigEntry> substitutions = new HashMap<String, ConfigEntry>();
  // these are the default configurations
  protected static Map<String, Object> defaults = new HashMap<>();

  // configurations for twister2
  // configurations with a default value should be specified as a ConfigEntry
  public static final ConfigEntry TWISTER2_HOME = new ConfigEntry(
      "twister2.directory.home", null, "TWISTER2_HOME");
  public static final ConfigEntry HOME = new ConfigEntry(
      "home", null, "HOME");
  public static final ConfigEntry TWISTER2_BIN = new ConfigEntry(
      "twister2.directory.bin", "${TWISTER2_HOME}/bin");
  public static final ConfigEntry TWISTER2_CONF = new ConfigEntry(
      "twister2.directory.conf", "${TWISTER2_HOME}/conf", null, "TWISTER2_CONF");
  public static final ConfigEntry TWISTER2_LIB = new ConfigEntry(
      "twister2.directory.lib", "${TWISTER2_HOME}/lib", null, "TWISTER2_LIB");
  public static final ConfigEntry TWISTER2_DIST = new ConfigEntry(
      "twister2.directory.dist", "${TWISTER2_HOME}/dist", null, "TWISTER_DIST");
  public static final ConfigEntry JAVA_HOME = new ConfigEntry(
      "twister2.directory.java.home", "${JAVA_HOME}", null, "JAVA_HOME");
  public static final ConfigEntry CLIENT_YAML = new ConfigEntry(
      "twister2.config.file.client.yaml", "${TWISTER2_CONF}/client.yaml");
  public static final ConfigEntry TASK_YAML = new ConfigEntry(
      "twister2.config.file.packing.yaml",   "${TWISTER2_CONF}/task.yaml");
  public static final ConfigEntry RESOURCE_SCHEDULER_YAML = new ConfigEntry(
      "twister2.config.file.scheduler.yaml", "${TWISTER2_CONF}/resource.yaml");
  public static final ConfigEntry NETWORK_YAML = new ConfigEntry(
      "twister2.config.file.network.yaml",    "${TWISTER2_CONF}/network.yaml");
  public static final ConfigEntry UPLOADER_YAML = new ConfigEntry(
      "twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/uploader.yaml");
  public static final ConfigEntry SYSTEM_YAML = new ConfigEntry(
      "twister2.config.file.system.yaml",  "${TWISTER2_CONF}/system.yaml");
  public static final ConfigEntry OVERRIDE_YAML = new ConfigEntry(
      "twister2.config.file.override.yaml",  "${TWISTER2_CONF}/override.yaml");
  public static final ConfigEntry CLUSTER_HOME = new ConfigEntry(
      "twister2.directory.cluster.home", "./core");
  public static final ConfigEntry CLUSTER_CONF = new ConfigEntry(
      "twister2.directory.cluster.conf", "./conf");
  public static final ConfigEntry VERBOSE = new ConfigEntry(
      "twister2.verbose", "false");
  public static final ConfigEntry JOB = new ConfigEntry(
      "twister2.job", null, "JOB");
  public static final ConfigEntry CLUSTER = new ConfigEntry(
      "twister2.cluster", null, "CLUSTER");
  public static final ConfigEntry AURORA_SCRIPT = new ConfigEntry(
      "twister2.resource.scheduler.aurora.script", "${TWISTER2_CONF}/twister2.aurora");

  public static final ConfigEntry DATA_YAML = new ConfigEntry(
      "twister2.config.file.data.yaml", "${TWISTER2_CONF}/data.yaml");

  public static final ConfigEntry HADOOP_HOME = new ConfigEntry(
      "twister2.hadoop.home", "${HADOOP_HOME}", null, "HADOOP_HOME");

  public static final String JOB_NAME = "twister2.job.name";

  // an internal property to represent the container id
  public static final String TWISTER2_CONTAINER_ID = "twister2.container.id";
  public static final String TWISTER2_CLUSTER_TYPE = "twister2.cluster.type";

  // job files will be packed in this directory in tar.gz file
  // job files will also be unpacked to this directory
  public static final String JOB_ARCHIVE_DIRECTORY = "twister2-job";

  public static final double TWISTER2_WORKER_CPU_DEFAULT = 1.0;
  public static final String TWISTER2_WORKER_CPU = "twister2.worker.cpu";

  // RAM in mega bytes
  public static final int TWISTER2_WORKER_RAM_DEFAULT = 200;
  public static final String TWISTER2_WORKER_RAM = "twister2.worker.ram";

  // volatile disk size per worker in GB
  public static final double WORKER_VOLATILE_DISK_DEFAULT = 0.0;
  public static final String WORKER_VOLATILE_DISK = "twister2.worker.volatile.disk";

  public static final int TWISTER2_WORKER_INSTANCES_DEFAULT = 1;
  public static final String TWISTER2_WORKER_INSTANCES = "twister2.worker.instances";


  static {
    substitutions.put("TWISTER2_HOME", TWISTER2_HOME);
    substitutions.put("HOME", HOME);
    substitutions.put("TWISTER2_CONF", TWISTER2_CONF);
    substitutions.put("TWISTER2_LIB", TWISTER2_LIB);
    substitutions.put("TWISTER2_DIST", TWISTER2_DIST);
    substitutions.put("TWISTER2_BIN", TWISTER2_BIN);
    substitutions.put("JAVA_HOME", JAVA_HOME);
    substitutions.put("JOB", JOB);
    substitutions.put("CLUSTER", CLUSTER);
    substitutions.put("HADOOP_HOME", HADOOP_HOME);
  }

  static {
    defaults.put(TWISTER2_BIN.getKey(), TWISTER2_BIN.getDefaultValue());
    defaults.put(TWISTER2_CONF.getKey(), TWISTER2_CONF.getDefaultValue());
    defaults.put(TWISTER2_LIB.getKey(), TWISTER2_LIB.getDefaultValue());
    defaults.put(TWISTER2_DIST.getKey(), TWISTER2_DIST.getDefaultValue());
    defaults.put(CLIENT_YAML.getKey(), CLIENT_YAML.getDefaultValue());
    defaults.put(TASK_YAML.getKey(), TASK_YAML.getDefaultValue());
    defaults.put(RESOURCE_SCHEDULER_YAML.getKey(), RESOURCE_SCHEDULER_YAML.getDefaultValue());
    defaults.put(NETWORK_YAML.getKey(), NETWORK_YAML.getDefaultValue());
    defaults.put(SYSTEM_YAML.getKey(), SYSTEM_YAML.getDefaultValue());
    defaults.put(UPLOADER_YAML.getKey(), UPLOADER_YAML.getDefaultValue());
    defaults.put(AURORA_SCRIPT.getKey(), AURORA_SCRIPT.getDefaultValue());
    defaults.put(DATA_YAML.getKey(), DATA_YAML.getDefaultValue());
  }

  protected Context() {
  }

  public static String taskConfigurationFile(Config cfg) {
    return cfg.getStringValue(TASK_YAML);
  }

  public static String networkConfigurationFile(Config cfg) {
    return cfg.getStringValue(NETWORK_YAML);
  }

  public static String uploaderConfigurationFile(Config cfg) {
    return cfg.getStringValue(UPLOADER_YAML);
  }

  public static String resourceSchedulerConfigurationFile(Config cfg) {
    return cfg.getStringValue(RESOURCE_SCHEDULER_YAML);
  }

  public static String clientConfigurationFile(Config cfg) {
    return cfg.getStringValue(CLIENT_YAML);
  }

  public static String systemConfigurationFile(Config cfg) {
    return cfg.getStringValue(SYSTEM_YAML);
  }

  public static String jobName(Config cfg) {
    return cfg.getStringValue(JOB_NAME);
  }

  public static String dataConfigurationFile(Config cfg) {
    return cfg.getStringValue(DATA_YAML);
  }

  public static String clusterType(Config cfg) {
    return cfg.getStringValue(TWISTER2_CLUSTER_TYPE);
  }

  public static String containerId(Config cfg) {
    return cfg.getStringValue(TWISTER2_CONTAINER_ID);
  }

  public static Boolean verbose(Config cfg) {
    return cfg.getBooleanValue(VERBOSE.getKey(), false);
  }

  public static String conf(Config cfg) {
    return cfg.getStringValue(TWISTER2_CONF);
  }

  public static String distDirectory(Config cfg) {
    return cfg.getStringValue(TWISTER2_DIST);
  }

  public static String libDirectory(Config cfg) {
    return cfg.getStringValue(TWISTER2_LIB);
  }

  public static String auroraScript(Config cfg) {
    return cfg.getStringValue(AURORA_SCRIPT);
  }

  public static String twister2Home(Config cfg) {
    return cfg.getStringValue(TWISTER2_HOME);
  }

  /**
   * CPU as double.
   * Can be any value more than 0.0
   * Examples: 0.2, 2.5, etc
   * @return
   */
  public static double workerCPU(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_WORKER_CPU, TWISTER2_WORKER_CPU_DEFAULT);
  }

  /**
   * RAM in Mega Bytes
   * @return
   */
  public static int workerRAM(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_WORKER_RAM, TWISTER2_WORKER_RAM_DEFAULT);
  }

  /**
   * Disk in Giga Bytes
   * @return
   */
  public static double workerVolatileDisk(Config cfg) {
    return cfg.getDoubleValue(WORKER_VOLATILE_DISK, WORKER_VOLATILE_DISK_DEFAULT);
  }

  public static int workerInstances(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_WORKER_INSTANCES, TWISTER2_WORKER_INSTANCES_DEFAULT);
  }
}
