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
package edu.iu.dsc.tws.tsched.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;

public class TaskAttributes {

  private static final Logger LOG = Logger.getLogger(TaskAttributes.class.getName());

  public Map<String, Double> getTaskRamMap(Set<Vertex> taskVertices) {

    Map<String, Double> taskRamMap = new HashMap<>();
    Object ram;
    double requiredRam = 0.0;
    for (Vertex task : taskVertices) {
      Config config = task.getConfig();
      if (config.get("Ram") != null) {
        ram = config.get("Ram");
        requiredRam = (double) ((Integer) ram);
      } else {
        requiredRam = TaskSchedulerContext.taskInstanceRam(config);
      }
      taskRamMap.put(task.getName(), requiredRam);
    }
    return taskRamMap;
  }

  public Map<String, Double> getTaskDiskMap(Set<Vertex> taskVertices) {

    Map<String, Double> taskDiskMap = new HashMap<>();
    Object disk;
    double requiredDisk = 0.0;
    for (Vertex task : taskVertices) {
      Config config = task.getConfig();
      if (config.get("Disk") != null) {
        disk = config.get("Disk");
        requiredDisk = (double) ((Integer) disk);
      } else {
        requiredDisk = TaskSchedulerContext.taskInstanceDisk(config);
      }
      taskDiskMap.put(task.getName(), requiredDisk);
    }
    return taskDiskMap;
  }

  public Map<String, Double> getTaskCPUMap(Set<Vertex> taskVertices) {

    Map<String, Double> taskCPUMap = new HashMap<>();
    Object cpu;
    double requiredCpu = 0.0;
    for (Vertex task : taskVertices) {
      Config config = task.getConfig();
      if (config.get("Cpu") != null) {
        cpu = config.get("Cpu");
        requiredCpu = (double) ((Integer) cpu);
      } else {
        requiredCpu = TaskSchedulerContext.taskInstanceCpu(config);
      }
      taskCPUMap.put(task.getName(), requiredCpu);
    }
    return taskCPUMap;
  }

  public int getTotalNumberOfInstances(Set<Vertex> iTaskSet) {

    HashMap<String, Integer> parallelTaskMap = getParallelTaskMap(iTaskSet);
    int totalNumberOfInstances = 0;
    for (int instances : parallelTaskMap.values()) {
      totalNumberOfInstances += instances;
    }
    return totalNumberOfInstances;
  }

  public HashMap<String, Integer> getParallelTaskMap(Set<Vertex> iTaskSet) {

    HashMap<String, Integer> parallelTaskMap = new HashMap<>();

    try {
      for (Vertex task : iTaskSet) {
        Config config = task.getConfig();
        String taskName = task.getName();
        Integer parallelTaskCount;
        if (task.getParallelism() >= 1) {
          parallelTaskCount = task.getParallelism();
        } else { //if (task.getParallelism() < 1) {
          parallelTaskCount = TaskSchedulerContext.taskParallelism(config);
        }
        parallelTaskMap.put(taskName, parallelTaskCount);
      }
    } catch (Exception ee) {
      ee.printStackTrace();
    }
    return parallelTaskMap;
  }
}
