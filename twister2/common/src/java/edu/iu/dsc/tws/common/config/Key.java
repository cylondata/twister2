//  Copyright 2017 Twitter. All rights reserved.
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
 * name() - return a string representation of the member name (e.g. HERON_HOME)
 * value() - return a key value bound to the enum (e.g. heron.directory.home)
 * getDefault() - return the default value bound to the enum
 * getType() - return the type of the key entry
 */
@SuppressWarnings({"checkstyle:MethodParamPad", "checkstyle:LineLength"})
public enum Key {

  //keys for heron environment
  HERON_HOME               ("heron.directory.home",      "/usr/local/heron"),
  HERON_BIN                ("heron.directory.bin",       "${HERON_HOME}/bin"),
  HERON_CONF               ("heron.directory.conf",      "${HERON_HOME}/conf"),
  HERON_LIB                ("heron.directory.lib",       "${HERON_HOME}/lib"),
  HERON_DIST               ("heron.directory.dist",      "${HERON_HOME}/dist"),
  HERON_ETC                ("heron.directory.etc",       "${HERON_HOME}/etc"),
  JAVA_HOME                ("heron.directory.java.home", "${JAVA_HOME}"),

  //keys for heron configuration files
  CLUSTER_YAML             ("heron.config.file.cluster.yaml",   "${HERON_CONF}/cluster.yaml"),
  CLIENT_YAML              ("heron.config.file.client.yaml",    "${HERON_CONF}/client.yaml"),
  METRICS_YAML             ("heron.config.file.metrics.yaml",   "${HERON_CONF}/metrics_sinks.yaml"),
  PACKING_YAML             ("heron.config.file.packing.yaml",   "${HERON_CONF}/packing.yaml"),
  SCHEDULER_YAML           ("heron.config.file.scheduler.yaml", "${HERON_CONF}/scheduler.yaml"),
  STATEMGR_YAML            ("heron.config.file.statemgr.yaml",  "${HERON_CONF}/statemgr.yaml"),
  SYSTEM_YAML              ("heron.config.file.system.yaml",    "${HERON_CONF}/heron_internals.yaml"),
  UPLOADER_YAML            ("heron.config.file.uploader.yaml",  "${HERON_CONF}/uploader.yaml"),

  //keys for config provided in the command line
  CLUSTER                  ("heron.config.cluster",             Type.STRING),
  ROLE                     ("heron.config.role",                Type.STRING),
  ENVIRON                  ("heron.config.environ",             Type.STRING),
  DRY_RUN                  ("heron.config.dry_run",             Boolean.FALSE),
  DRY_RUN_FORMAT_TYPE      ("heron.config.dry_run_format_type", Type.DRY_RUN_FORMAT_TYPE),
  VERBOSE                  ("heron.config.verbose",             Boolean.FALSE),
  CONFIG_PROPERTY          ("heron.config.property",            Type.STRING);


  private final String value;
  private final Object defaultValue;
  private final Type type;

  public enum Type {
    BOOLEAN,
    BYTE_AMOUNT,
    DOUBLE,
    DRY_RUN_FORMAT_TYPE,
    INTEGER,
    LONG,
    STRING,
    PACKAGE_TYPE,
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
   * Get the key value for this enum (i.e., heron.directory.home)
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
