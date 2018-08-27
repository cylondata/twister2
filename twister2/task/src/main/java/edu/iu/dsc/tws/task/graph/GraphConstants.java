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
package edu.iu.dsc.tws.task.graph;

import edu.iu.dsc.tws.common.config.Config;

public class GraphConstants {

  protected GraphConstants() {
  }

  public static final String TWISTER2_TASK_INSTANCE_RAM = "Ram"; //In MB
  public static final int TWISTER2_TASK_INSTANCE_RAM_DEFAULT = 512;

  public static final String TWISTER2_TASK_INSTANCE_DISK = "Disk"; //In GB
  public static final int TWISTER2_TASK_INSTANCE_DISK_DEFAULT = 500;

  public static final String TWISTER2_TASK_INSTANCE_CPU = "Cpu";
  public static final int TWISTER2_TASK_INSTANCE_CPU_DEFAULT = 2;

  public static int taskInstanceRam(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_TASK_INSTANCE_RAM, TWISTER2_TASK_INSTANCE_RAM_DEFAULT);
  }

  public static int taskInstanceDisk(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_TASK_INSTANCE_DISK, TWISTER2_TASK_INSTANCE_DISK_DEFAULT);
  }

  public static int taskInstanceCpu(Config cfg) {
    return cfg.getIntegerValue(TWISTER2_TASK_INSTANCE_CPU, TWISTER2_TASK_INSTANCE_CPU_DEFAULT);
  }
}
