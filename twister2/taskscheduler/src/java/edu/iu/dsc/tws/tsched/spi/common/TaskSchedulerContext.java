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
package edu.iu.dsc.tws.tsched.spi.common;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

/**
 * This class is to represent the default values for the task instances and task scheduler logical
 * container values.
 */
public class TaskSchedulerContext extends Context {

  public static final String TWISTER2_STREAMING_TASK_SCHEDULING_MODE = "twister2.streaming"
          + ".taskscheduler";
  public static final String TWISTER2_STREAMING_TASK_SCHEDULING_MODE_DEFAULT = "roundrobin";

  public static final String TWISTER2_BATCH_TASK_SCHEDULING_MODE = "twister2.batch"
          + ".taskscheduler";
  public static final String TWISTER2_BATCH_TASK_SCHEDULING_MODE_DEFAULT = "roundrobin";

  public static final String TWISTER2_STREAMING_TASK_SCHEDULING_CLASS =
          "twister2.streaming.taskscheduler.class";
  public static final String TWISTER2_STREAMING_TASK_SCHEDULING_CLASS_DEFAULT =
          "edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler";

  public static final String TWISTER2_BATCH_TASK_SCHEDULING_CLASS =
          "twister2.batch.taskscheduler.class";
  public static final String TWISTER2_BATCH_TASK_SCHEDULING_CLASS_DEFAULT =
          "edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler";

  public static final String TWISTER2_TASK_TYPE = "twister2.task.type";
  public static final String TWISTER2_TASK_TYPE_DEFAULT = "streaming";

  public static final String TWISTER2_TASK_INSTANCE_RAM = "twister2.task.instance.ram";
  public static final double TWISTER2_TASK_INSTANCE_RAM_DEFAULT = 512.0;

  public static final String TWISTER2_TASK_INSTANCE_DISK = "twister2.task.instance.disk";
  public static final double TWISTER2_TASK_INSTANCE_DISK_DEFAULT = 500.0;

  public static final String TWISTER2_TASK_INSTANCE_CPU = "twister2.task.instance.cpu";
  public static final double TWISTER2_TASK_INSTANCE_CPU_DEFAULT = 2.0;

  public static final String TWISTER2_TASK_INSTANCE_NETWORK = "twister2.task.instance.network";
  public static final double TWISTER2_TASK_INSTANCE_NETWORK_DEFAULT = 512.0;

  public static final String TWISTER2_CONTAINER_INSTANCE_RAM = "twister2.container.instance.ram";
  public static final double TWISTER2_CONTAINER_INSTANCE_RAM_DEFAULT = 1024.0;

  public static final String TWISTER2_CONTAINER_INSTANCE_DISK = "twister2.container.instance.disk";
  public static final double TWISTER2_CONTAINER_INSTANCE_DISK_DEFAULT = 1000.0;

  public static final String TWISTER2_CONTAINER_INSTANCE_CPU = "twister2.container.instance.cpu";
  public static final double TWISTER2_CONTAINER_INSTANCE_CPU_DEFAULT = 2.0;

  public static final String TWISTER2_CONTAINER_INSTANCE_NETWORK
          = "twister2.container.instance.network";
  public static final double TWISTER2_CONTAINER_INSTANCE_NETWORK_DEFAULT = 1024;

  public static final String TWISTER2_TASK_PARALLELISM = "twister2.task.parallelism";
  public static final int TWISTER2_TASK_PARALLELISM_DEFAULT = 2;

  public static final String TWISTER2_NO_OF_INSTANCES_PER_CONTAINER
          = "twister2.task.instances";
  public static final int TWISTER2_NO_OF_INSTANCES_PER_CONTAINER_DEFAULT = 2;

  public static final String TWISTER2_RAM_PADDING_PER_CONTAINER
          = "twister2.ram.padding.container";
  public static final double TWISTER2_RAM_PADDING_PER_CONTAINER_DEFAULT = 2.0;

  public static final String TWISTER2_DISK_PADDING_PER_CONTAINER
          = "twister2.disk.padding.container";
  public static final double TWISTER2_DISK_PADDING_PER_CONTAINER_DEFAULT = 12.0;

  public static final String TWISTER2_CPU_PADDING_PER_CONTAINER
          = "twister2.cpu.padding.container";
  public static final double TWISTER2_CPU_PADDING_PER_CONTAINER_DEFAULT = 1.0;

  public static final String TWISTER2_CONTAINER_PADDING_PERCENTAGE
          = "twister2.container.padding.percentage";
  public static final int TWISTER2_CONTAINER_PADDING_PERCENTAGE_DEFAULT = 1;

  public static final String TWISTER2_CONTAINER_INSTANCE_BANDWIDTH
          = "twister2.container.instance.bandwidth";
  public static final double TWISTER2_CONTAINER_INSTANCE_BANDWIDTH_DEFAULT = 100; //Mbps

  public static final String TWISTER2_CONTAINER_INSTANCE_LATENCY
          = "twister2.container.instance.latency";
  public static final double TWISTER2_CONTAINER_INSTANCE_LATENCY_DEFAULT = 0.02; //Milliseconds

  public static final String TWISTER2_DATANODE_INSTANCE_BANDWIDTH
          = "twister2.datanode.instance.bandwidth";
  public static final double TWISTER2_DATANODE_INSTANCE_BANDWIDTH_DEFAULT = 200; //Mbps

  public static final String TWISTER2_DATANODE_INSTANCE_LATENCY
          = "twister2.datanode.instance.latency";
  public static final double TWISTER2_DATANODE_INSTANCE_LATENCY_DEFAULT = 0.01; //Milliseconds

  public static String streamingTaskSchedulingMode(Config cfg) {
    return cfg.getStringValue(TWISTER2_STREAMING_TASK_SCHEDULING_MODE,
            TWISTER2_STREAMING_TASK_SCHEDULING_MODE_DEFAULT);
  }

  public static String batchTaskSchedulingMode(Config cfg) {
    return cfg.getStringValue(TWISTER2_BATCH_TASK_SCHEDULING_MODE,
            TWISTER2_BATCH_TASK_SCHEDULING_MODE_DEFAULT);
  }

  public static String streamingTaskSchedulingClass(Config cfg) {
    return cfg.getStringValue(TWISTER2_STREAMING_TASK_SCHEDULING_CLASS,
            TWISTER2_STREAMING_TASK_SCHEDULING_CLASS_DEFAULT);
  }

  public static String batchTaskSchedulingClass(Config cfg) {
    return cfg.getStringValue(TWISTER2_BATCH_TASK_SCHEDULING_CLASS,
            TWISTER2_BATCH_TASK_SCHEDULING_CLASS_DEFAULT);
  }

  public static String taskType(Config cfg) {
    return cfg.getStringValue(TWISTER2_TASK_TYPE, TWISTER2_TASK_TYPE_DEFAULT);
  }

  public static double taskInstanceRam(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_TASK_INSTANCE_RAM, TWISTER2_TASK_INSTANCE_RAM_DEFAULT);
  }

  public static double taskInstanceDisk(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_TASK_INSTANCE_DISK, TWISTER2_TASK_INSTANCE_DISK_DEFAULT);
  }

  public static double taskInstanceCpu(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_TASK_INSTANCE_CPU, TWISTER2_TASK_INSTANCE_CPU_DEFAULT);
  }

  public static double taskInstanceNetwork(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_TASK_INSTANCE_NETWORK,
            TWISTER2_TASK_INSTANCE_NETWORK_DEFAULT);
  }

  public static double containerInstanceRam(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_CONTAINER_INSTANCE_RAM,
            TWISTER2_CONTAINER_INSTANCE_RAM_DEFAULT);
  }

  public static double containerInstanceDisk(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_CONTAINER_INSTANCE_DISK,
            TWISTER2_CONTAINER_INSTANCE_DISK_DEFAULT);
  }

  public static double containerInstanceCpu(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_CONTAINER_INSTANCE_CPU,
            TWISTER2_CONTAINER_INSTANCE_CPU_DEFAULT);
  }

  public static double containerInstanceNetwork(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_CONTAINER_INSTANCE_NETWORK,
            TWISTER2_CONTAINER_INSTANCE_NETWORK_DEFAULT);
  }

  public static int taskParallelism(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_TASK_PARALLELISM,
            TWISTER2_TASK_PARALLELISM_DEFAULT);
  }

  public static int defaultTaskInstancesPerContainer(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_NO_OF_INSTANCES_PER_CONTAINER,
            TWISTER2_NO_OF_INSTANCES_PER_CONTAINER_DEFAULT);
  }

  public static double containerRamPadding(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_RAM_PADDING_PER_CONTAINER,
            TWISTER2_RAM_PADDING_PER_CONTAINER_DEFAULT);
  }

  public static double containerDiskPadding(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_DISK_PADDING_PER_CONTAINER,
            TWISTER2_DISK_PADDING_PER_CONTAINER_DEFAULT);
  }

  public static double containerCpuPadding(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_CPU_PADDING_PER_CONTAINER,
            TWISTER2_CPU_PADDING_PER_CONTAINER_DEFAULT);
  }

  public static int containerPaddingPercentage(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_CONTAINER_PADDING_PERCENTAGE,
            TWISTER2_CONTAINER_PADDING_PERCENTAGE_DEFAULT);
  }

  public static double datanodeInstanceBandwidth(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_DATANODE_INSTANCE_BANDWIDTH,
            TWISTER2_DATANODE_INSTANCE_BANDWIDTH_DEFAULT);
  }

  public static double datanodeInstanceLatency(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_DATANODE_INSTANCE_LATENCY,
            TWISTER2_DATANODE_INSTANCE_LATENCY_DEFAULT);
  }

  public static double containerInstanceBandwidth(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_CONTAINER_INSTANCE_BANDWIDTH,
            TWISTER2_CONTAINER_INSTANCE_BANDWIDTH_DEFAULT);
  }

  public static double containerInstanceLatency(Config cfg) {
    return cfg.getDoubleValue(TWISTER2_CONTAINER_INSTANCE_LATENCY,
            TWISTER2_CONTAINER_INSTANCE_LATENCY_DEFAULT);
  }
}

