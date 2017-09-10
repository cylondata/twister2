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

public class Context {
  // configurations for twister2
  public static final String TWISTER2_HOME = "twister2.directory.home";
  public static final String TWISTER2_BIN = "twister2.directory.bin";       "${TWISTER2_HOME}/bin"),
  public static final String TWISTER2_CONF = "twister2.directory.conf";      "${TWISTER2_HOME}/conf"),
  public static final String TWISTER2_LIB = "twister2.directory.lib";       "${TWISTER2_HOME}/lib"),
  public static final String TWISTER2_DIST = "twister2.directory.dist";      "${TWISTER2_HOME}/dist"),
  TWISTER2_ETC                ("twister2.directory.etc",       "${TWISTER2_HOME}/etc"),
  JAVA_HOME                ("twister2.directory.java.home", "${JAVA_HOME}"),

  //keys for twister2 configuration files
  CLIENT_YAML            ("twister2.config.file.client.yaml",    "${TWISTER2_CONF}/client.yaml"),
  TASK_SCHEDULER_YAML    ("twister2.config.file.packing.yaml",   "${TWISTER2_CONF}/task.yaml"),
  RESOURCE_SCHEDULER_YAML("twister2.config.file.scheduler.yaml",
                              "),
  STATEMGR_YAML          ("twister2.config.file.statemgr.yaml",  "${TWISTER2_CONF}/statemgr.yaml"),
  NETWORK_YAML           ("twister2.config.file.system.yaml",    "${TWISTER2_CONF}/network.yaml"),
  UPLOADER_YAML          ("twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/uploader.yaml"),
  SYSTEM_YAML            ("twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/system.yaml"),

  TWISTER2_CLUSTER_HOME  ("twister2.directory.cluster.home",      "./twister2-core"),
  TWISTER2_CLUSTER_CONF  ("twister2.directory.cluster.conf",      "./twister2-conf"),
  OVERRIDE_YAML("twister2.config.file.override.yaml",  "${TWISTER2_CONF}/override.yaml");

  protected Context() {
  }

  public static String taskConfigurationFile(Config cfg) {
    return cfg.getStringValue(TASK_SCHEDULER_YAML, );
  }

  public static String networkConfigurationFile(Config cfg) {
    return cfg.getStringValue(NETWORK_YAML);
  }

  public static String uploaderConfigurationFile(Config cfg) {
    return cfg.getStringValue(UPLOADER_YAML);
  }

  public static String stateManagerConfigurationFile(Config cfg) {
    return cfg.getStringValue(STATEMGR_YAML);
  }

  public static String resourceSchedulerConfigurationFile(Config cfg) {
    return cfg.getStringValue(RESOURCE_SCHEDULER_YAML);
  }

  public static String clientConfigurationFile(Config cfg) {
    return cfg.getStringValue(CLIENT_YAML);
  }
}
