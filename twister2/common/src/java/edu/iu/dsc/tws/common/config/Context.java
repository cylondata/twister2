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
  protected static Map<String, String> configurationDefaults = new HashMap<String, String>();

  // configurations for twister2
  public static final ConfigEntry twister2Home = new ConfigEntry("twister2.directory.home");
  public static final ConfigEntry twister2Bin = new ConfigEntry(
      "twister2.directory.bin", "${TWISTER2_HOME}/bin");
  public static final ConfigEntry twister2Conf = new ConfigEntry(
      "twister2.directory.conf", "${TWISTER2_HOME}/conf");
  public static final ConfigEntry twister2Lib = new ConfigEntry(
      "twister2.directory.lib", "${TWISTER2_HOME}/lib");
  public static final ConfigEntry twister2Dist = new ConfigEntry(
      "twister2.directory.dist", "${TWISTER2_HOME}/dist");
  public static final ConfigEntry  javaHome = new ConfigEntry(
      "twister2.directory.java.home", "${JAVA_HOME}");
  public static final ConfigEntry clientYaml = new ConfigEntry(
      "twister2.config.file.client.yaml", "${TWISTER2_CONF}/client.yaml");
  public static final ConfigEntry schedulerYaml = new ConfigEntry(
      "twister2.config.file.packing.yaml",   "${TWISTER2_CONF}/task.yaml");
  public static final ConfigEntry resourceSchedulerYaml = new ConfigEntry(
      "twister2.config.file.scheduler.yaml", "${TWISTER2_CONF}/resource-scheduler.yaml");
  public static final ConfigEntry networkYaml = new ConfigEntry(
      "twister2.config.file.system.yaml",    "${TWISTER2_CONF}/network.yaml");
  public static final ConfigEntry uploaderYaml = new ConfigEntry(
      "twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/uploader.yaml");
  public static final ConfigEntry systemYaml = new ConfigEntry(
      "twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/system.yaml");
  public static final ConfigEntry overrideYaml = new ConfigEntry(
      "twister2.config.file.override.yaml",  "${TWISTER2_CONF}/override.yaml");
  public static final ConfigEntry clusterHome = new ConfigEntry(
      "twister2.directory.cluster.home", "./heron-core");
  public static final ConfigEntry clusterConf = new ConfigEntry(
      "heron.directory.cluster.conf", "./heron-conf");

  protected Context() {

  }

  public static String taskConfigurationFile(Config cfg) {
    return cfg.getStringValue(schedulerYaml);
  }

  public static String networkConfigurationFile(Config cfg) {
    return cfg.getStringValue(networkYaml);
  }

  public static String uploaderConfigurationFile(Config cfg) {
    return cfg.getStringValue(uploaderYaml);
  }

  public static String resourceSchedulerConfigurationFile(Config cfg) {
    return cfg.getStringValue(resourceSchedulerYaml);
  }

  public static String clientConfigurationFile(Config cfg) {
    return cfg.getStringValue(clientYaml);
  }
}
