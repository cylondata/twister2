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
  protected static Map<String, ConfigEntry> substitutions = new HashMap<String, ConfigEntry>();

  // configurations for twister2
  // configurations with a default value should be specified as a ConfigEntry
  public static final ConfigEntry TWISTER2_HOME = new ConfigEntry(
      "twister2.directory.home", null, "TWISTER2_HOME");
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
  public static final ConfigEntry SCHEDULER_YAML = new ConfigEntry(
      "twister2.config.file.packing.yaml",   "${TWISTER2_CONF}/task.yaml");
  public static final ConfigEntry RESOURCE_SCHEDULER_YAML = new ConfigEntry(
      "twister2.config.file.scheduler.yaml", "${TWISTER2_CONF}/resource-scheduler.yaml");
  public static final ConfigEntry NETWORK_YAML = new ConfigEntry(
      "twister2.config.file.system.yaml",    "${TWISTER2_CONF}/network.yaml");
  public static final ConfigEntry UPLOADER_YAML = new ConfigEntry(
      "twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/uploader.yaml");
  public static final ConfigEntry SYSTEM_YAML = new ConfigEntry(
      "twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/system.yaml");
  public static final ConfigEntry OVERRIDE_YAML = new ConfigEntry(
      "twister2.config.file.override.yaml",  "${TWISTER2_CONF}/override.yaml");
  public static final ConfigEntry CLUSTER_HOME = new ConfigEntry(
      "twister2.directory.cluster.home", "./heron-core");
  public static final ConfigEntry CLUSTER_CONF = new ConfigEntry(
      "heron.directory.cluster.conf", "./heron-conf");

  // an internal property to represent the container id
  public static final String TWISTER2_CONTAINER_ID = "twister2.container.id";
  public static final String TWISTER2_CLUSTER_NAME = "twister2.cluster.name";

  static {
    substitutions.put("TWISTER2_HOME", TWISTER2_HOME);
    substitutions.put("TWISTER2_CONF", TWISTER2_CONF);
    substitutions.put("TWISTER2_LIB", TWISTER2_LIB);
    substitutions.put("TWISTER2_DIST", TWISTER2_DIST);
    substitutions.put("JAVA_HOME", JAVA_HOME);
  }

  protected Context() {

  }

  public static String taskConfigurationFile(Config cfg) {
    return cfg.getStringValue(SCHEDULER_YAML);
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

  public static String clusterName(Config cfg) {
    return cfg.getStringValue(TWISTER2_CLUSTER_NAME);
  }

  public static String containerId(Config cfg) {
    return cfg.getStringValue(TWISTER2_CONTAINER_ID);
  }
}
