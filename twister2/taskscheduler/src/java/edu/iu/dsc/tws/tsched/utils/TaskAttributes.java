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

public class TaskAttributes {

  private static final Logger LOG = Logger.getLogger(TaskAttributes.class.getName());

  public Map<String, Double> getTaskRamMap(Set<Vertex> taskVertices) {

    Map<String, Double> taskRamMap = new HashMap<>();
    for (Vertex task : taskVertices) {
      Config config = task.getConfig();
      Object ram = config.get("Ram");
      double requiredRam = (double) ((Integer) ram);
      taskRamMap.put(task.getName(), requiredRam);
      //LOG.info("Task Name:" + task.getName() + "\t" + "Required Ram:" + requiredRam);
    }
    return taskRamMap;
  }

  public Map<String, Double> getTaskDiskMap(Set<Vertex> taskVertices) {

    Map<String, Double> taskDiskMap = new HashMap<>();
    for (Vertex task : taskVertices) {
      Config config = task.getConfig();
      Object disk = config.get("Disk");
      double requiredDisk = (double) ((Integer) disk);
      taskDiskMap.put(task.getName(), requiredDisk);
      //LOG.info("Task Name:" + task.getName() + "\t" + "Required Disk:" + requiredDisk);
    }
    return taskDiskMap;
  }

  public Map<String, Double> getTaskCPUMap(Set<Vertex> taskVertices) {

    Map<String, Double> taskCPUMap = new HashMap<>();
    for (Vertex task : taskVertices) {
      Config config = task.getConfig();
      Object cpu = config.get("Cpu");
      double requiredCpu = (double) ((Integer) cpu);
      taskCPUMap.put(task.getName(), requiredCpu);
      //LOG.info("Task Name:" + task.getName() + "\t" + "Required Cpu:" + requiredCpu);
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
        String taskName = task.getName();
        Integer parallelTaskCount = task.getParallelism();
        //LOG.info("Task Name:" + taskName + "\t" + "parallel task count:" + parallelTaskCount);
        parallelTaskMap.put(taskName, parallelTaskCount);
      }
    } catch (Exception ee) {
      ee.printStackTrace();
    }
    return parallelTaskMap;
  }
}
