//
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

/**
 * Enum of all configuration key values. The following methods exist:
 *
 * name() - return a string representation of the member name (e.g. TWISTER2_HOME)
 * value() - return a key value bound to the enum (e.g. twister2.directory.home)
 * getDefault() - return the default value bound to the enum
 * getType() - return the type of the key entry
 */
@SuppressWarnings({"checkstyle:MethodParamPad", "checkstyle:LineLength"})
public enum Key {

  //keys for twister2 environment
  TWISTER2_HOME               ("twister2.directory.home",      "/usr/local/twister2"),
  TWISTER2_BIN                ("twister2.directory.bin",       "${TWISTER2_HOME}/bin"),
  TWISTER2_CONF               ("twister2.directory.conf",      "${TWISTER2_HOME}/conf"),
  TWISTER2_LIB                ("twister2.directory.lib",       "${TWISTER2_HOME}/lib"),
  TWISTER2_DIST               ("twister2.directory.dist",      "${TWISTER2_HOME}/dist"),
  TWISTER2_ETC                ("twister2.directory.etc",       "${TWISTER2_HOME}/etc"),
  JAVA_HOME                ("twister2.directory.java.home", "${JAVA_HOME}"),

  //keys for twister2 configuration files
  CLIENT_YAML            ("twister2.config.file.client.yaml",    "${TWISTER2_CONF}/client.yaml"),
  TASK_SCHEDULER_YAML    ("twister2.config.file.packing.yaml",   "${TWISTER2_CONF}/task.yaml"),
  RESOURCE_SCHEDULER_YAML("twister2.config.file.scheduler.yaml",
      "${TWISTER2_CONF}/resource_scheduler.yaml"),
  STATEMGR_YAML          ("twister2.config.file.statemgr.yaml",  "${TWISTER2_CONF}/statemgr.yaml"),
  NETWORK_YAML           ("twister2.config.file.system.yaml",    "${TWISTER2_CONF}/network.yaml"),
  UPLOADER_YAML          ("twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/uploader.yaml"),
  SYSTEM_YAML            ("twister2.config.file.uploader.yaml",  "${TWISTER2_CONF}/system.yaml"),

  TWISTER2_CLUSTER_HOME  ("twister2.directory.cluster.home",      "./twister2-core"),
  TWISTER2_CLUSTER_CONF  ("twister2.directory.cluster.conf",      "./twister2-conf"),
  OVERRIDE_YAML("twister2.config.file.override.yaml",  "${TWISTER2_CONF}/override.yaml");


  private final String value;
  private final Object defaultValue;
  private final Type type;

  public enum Type {
    BOOLEAN,
    DOUBLE,
    INTEGER,
    LONG,
    STRING,
    PROPERTIES,
    UNKNOWN
  }

  Key(String value, Type type) {
    this.value = value;
    this.type = type;
    this.defaultValue = null;
  }

  Key(String value, String defaultValue) {
    this.value = value;
    this.type = Type.STRING;
    this.defaultValue = defaultValue;
  }

  Key(String value, Double defaultValue) {
    this.value = value;
    this.type = Type.DOUBLE;
    this.defaultValue = defaultValue;
  }

  Key(String value, Boolean defaultValue) {
    this.value = value;
    this.type = Type.BOOLEAN;
    this.defaultValue = defaultValue;
  }

  /**
   * Get the key value for this enum (i.e., twister2.directory.home)
   * @return key value
   */
  public String value() {
    return value;
  }

  public Type getType() {
    return type;
  }

  /**
   * Return the default value
   */
  public Object getDefault() {
    return this.defaultValue;
  }

  public String getDefaultString() {
    if (type != Type.STRING) {
      throw new IllegalAccessError(String.format(
          "Config Key %s is type %s, getDefaultString() not supported", this.name(), this.type));
    }
    return (String) this.defaultValue;
  }
}
