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
package edu.iu.dsc.tws.api.config;

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
  public static final ConfigEntry TASK_YAML = new ConfigEntry(
      "twister2.config.file.packing.yaml", "${TWISTER2_CONF}/task.yaml");
  public static final ConfigEntry RESOURCE_SCHEDULER_YAML = new ConfigEntry(
      "twister2.config.file.scheduler.yaml", "${TWISTER2_CONF}/resource.yaml");
  public static final ConfigEntry NETWORK_YAML = new ConfigEntry(
      "twister2.config.file.network.yaml", "${TWISTER2_CONF}/network.yaml");
  public static final ConfigEntry CORE_YAML = new ConfigEntry(
      "twister2.config.file.core.yaml", "${TWISTER2_CONF}/core.yaml");
  public static final ConfigEntry OVERRIDE_YAML = new ConfigEntry(
      "twister2.config.file.override.yaml", "${TWISTER2_CONF}/override.yaml");
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

  public static final ConfigEntry CHECKPOINT_YAML = new ConfigEntry(
      "twister2.config.file.checkpoint.yaml", "${TWISTER2_CONF}/checkpoint.yaml");

  public static final ConfigEntry DATA_YAML = new ConfigEntry(
      "twister2.config.file.data.yaml", "${TWISTER2_CONF}/data.yaml");

  public static final ConfigEntry HADOOP_HOME = new ConfigEntry(
      "twister2.hadoop.home", "${HADOOP_HOME}", null, "HADOOP_HOME");

  public static final String JOB_NAME = "twister2.job.name";
  public static final String JOB_OBJECT = "twister2.job.object";
  public static final String JOB_ID = "twister2.job.id";

  // an internal property to represent the container id
  public static final String TWISTER2_CONTAINER_ID = "twister2.container.id";
  public static final String TWISTER2_CLUSTER_TYPE = "twister2.cluster.type";

  // job files will be packed in this directory in tar.gz file
  // job files will also be unpacked to this directory
  public static final String JOB_ARCHIVE_DIRECTORY = "twister2-job";

  public static final int TWISTER2_WORKER_INSTANCES_DEFAULT = 1;
  public static final String TWISTER2_WORKER_INSTANCES = "twister2.worker.instances";

  public static final String TWISTER2_DIRECT_EDGE = "direct";

  public static final String TWISTER2_DATA_INPUT = "generate"; // or "read"

  public static final String TWISTER2_LOCAL_FILESYSTEM = "local";

  public static final String TWISTER2_HDFS_FILESYSTEM = "hdfs";

  public static final String TWISTER2_BANDWIDTH = "bandwidth";

  public static final String TWISTER2_LATENCY = "latency";

  public static final String TWISTER2_MAX_TASK_INSTANCES_PER_WORKER
      = "twister2.max.task.instances.per.worker";

  public static final String TWISTER2_TASK_INSTANCE_ODD_PARALLELISM
      = "twister2.task.instance.odd.parallelism";

  /**
   * Name of the operation in the current configuration
   */
  public static final String OPERATION_NAME = "opname";

  /**
   * If it is streaming environment, this property will be set
   */
  public static final String STREAMING = "streaming";

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
    defaults.put(TASK_YAML.getKey(), TASK_YAML.getDefaultValue());
    defaults.put(RESOURCE_SCHEDULER_YAML.getKey(), RESOURCE_SCHEDULER_YAML.getDefaultValue());
    defaults.put(NETWORK_YAML.getKey(), NETWORK_YAML.getDefaultValue());
    defaults.put(CORE_YAML.getKey(), CORE_YAML.getDefaultValue());
    defaults.put(AURORA_SCRIPT.getKey(), AURORA_SCRIPT.getDefaultValue());
    defaults.put(CHECKPOINT_YAML.getKey(), CHECKPOINT_YAML.getDefaultValue());
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

  public static String resourceSchedulerConfigurationFile(Config cfg) {
    return cfg.getStringValue(RESOURCE_SCHEDULER_YAML);
  }

  public static String systemConfigurationFile(Config cfg) {
    return cfg.getStringValue(CORE_YAML);
  }

  public static String jobName(Config cfg) {
    return cfg.getStringValue(JOB_NAME);
  }

  public static String jobId(Config cfg) {
    return cfg.getStringValue(JOB_ID);
  }

  public static String dataConfigurationFile(Config cfg) {
    return cfg.getStringValue(DATA_YAML);
  }

  public static String checkpointCofigurationFile(Config cfg) {
    return cfg.getStringValue(CHECKPOINT_YAML);
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

  public static String checkpointConfigurationFile(Config cfg) {
    return cfg.getStringValue(CHECKPOINT_YAML);
  }

  public static int workerInstances(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_WORKER_INSTANCES, TWISTER2_WORKER_INSTANCES_DEFAULT);
  }

  public static Map<String, Object> getDefaults() {
    return defaults;
  }


  public static String getStringPropertyValue(Config cfg, String name, String def) {
    String first = cfg.getStringValue(name, def);

    Object modSpecific = getModeProperty(cfg, name, first);
    first = modSpecific.toString();

    String second = cfg.getStringValue(
        getModeSpecificPropertyName(cfg, name), first);

    Object modeOpSpecific = getModeOpProperty(cfg, name, second);
    second = modeOpSpecific.toString();

    return cfg.getStringValue(
        getOpSpecificPropertyName(cfg, name), second);
  }

  public static long getLongPropertyValue(Config cfg, String name, long def) {
    long first = cfg.getLongValue(name, def);

    Object modSpecific = getModeProperty(cfg, name, first);
    first = TypeUtils.getLong(modSpecific);

    long second = cfg.getLongValue(
        getModeSpecificPropertyName(cfg, name), first);

    Object modeOpSpecific = getModeOpProperty(cfg, name, second);
    second = TypeUtils.getLong(modeOpSpecific);

    return cfg.getLongValue(
        getOpSpecificPropertyName(cfg, name), second);
  }

  public static double getDoublePropertyValue(Config cfg, String name, double def) {
    double first = cfg.getDoubleValue(name, def);

    Object modSpecific = getModeProperty(cfg, name, first);
    first = TypeUtils.getDouble(modSpecific);

    double second = cfg.getDoubleValue(
        getModeSpecificPropertyName(cfg, name), first);

    Object modeOpSpecific = getModeOpProperty(cfg, name, second);
    second = TypeUtils.getDouble(modeOpSpecific);

    return cfg.getDoubleValue(
        getOpSpecificPropertyName(cfg, name), second);
  }

  public static int getIntPropertyValue(Config cfg, String name, int def) {

    // we get the first value
    int first = cfg.getIntegerValue(name, def);

    Object modSpecific = getModeProperty(cfg, name, first);
    first = TypeUtils.getInteger(modSpecific);

    int second = cfg.getIntegerValue(
        getModeSpecificPropertyName(cfg, name), first);

    Object modeOpSpecific = getModeOpProperty(cfg, name, second);
    second = TypeUtils.getInteger(modeOpSpecific);


    return cfg.getIntegerValue(
        getOpSpecificPropertyName(cfg, name), second);
  }

  private static Object getModeOpProperty(Config cfg, String name, Object def) {
    String mode = mode(cfg);

    Object o = cfg.get(mode);
    if (o != null) {
      if (o instanceof Map) {
        // now check if the op is present
        String op = cfg.getStringValue(OPERATION_NAME, "");
        Object opMap = ((Map) o).get(op);
        if (opMap != null) {
          if (opMap instanceof Map) {
            Object val = ((Map) opMap).get(name);
            if (val != null) {
              return val;
            }
          } else {
            throw new RuntimeException("Operation specific configurations should be a map");
          }
        }
        return def;
      } else {
        throw new RuntimeException("stream should be a map property");
      }
    }
    return def;
  }

  private static Object getModeProperty(Config cfg, String name, Object def) {
    String mode = mode(cfg);

    Object o = cfg.get(mode);
    if (o != null) {
      if (o instanceof Map) {
        Object first = ((Map) o).get(name);
        if (first != null) {
          return first;
        }
        return def;
      } else {
        throw new RuntimeException("stream should be a map property");
      }
    }
    return def;
  }

  private static String mode(Config cfg) {
    String mode = "batch";
    boolean stream = cfg.getBooleanValue(STREAMING, false);
    if (stream) {
      mode = "stream";
    }
    return mode;
  }

  private static String getModeSpecificPropertyName(Config cfg, String name) {
    boolean stream = cfg.getBooleanValue(STREAMING, false);
    if (stream) {
      return name + "." + "stream";
    } else {
      return name + "." + "batch";
    }
  }

  private static String getOpSpecificPropertyName(Config cfg, String name) {
    boolean stream = cfg.getBooleanValue(STREAMING, false);
    String op = cfg.getStringValue(OPERATION_NAME, "");
    if (stream) {
      return name + "." + "stream" + "." + op;
    } else {
      return name + "." + "batch" + "." + op;
    }
  }

}
