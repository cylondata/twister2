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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

//This class will be replaced with the original JobAttributes file from the job package.

public final class JobAttributes {

  private static final Logger LOG = Logger.getLogger(JobAttributes.class.getName());

  public static final int JOB_CONTAINER_PADDING_PERCENTAGE = 10;
  public static final Double JOB_CONTAINER_MAX_RAM_VALUE = 20.00;
  public static final Double JOB_CONTAINER_MAX_DISK_VALUE = 200.00;
  public static final Double JOB_CONTAINER_MAX_CPU_VALUE = 5.0;
  public static int numberOfContainers;
  public static int numberOfInstances;

  private JobAttributes() {
  }

  public static int getNumberOfContainers(Job job) {
    setNumberOfContainers();
    return numberOfContainers;
  }

  public static void setNumberOfContainers() {
    numberOfContainers = Integer.parseInt(JobConfig.numberOfContainers.trim());
  }

  public static int getNumberOfInstances(Job job) {
    return numberOfInstances;
  }

  public static void setNumberOfInstances() {
    numberOfInstances = Integer.parseInt(JobConfig.numberOfInstances.trim());
  }

  public static Map<String, Double> getTaskRamMap(Job job) {

    Set<String> taskNameSet = new HashSet<String>();
    taskNameSet.add("mpitask1");
    taskNameSet.add("mpitask2");
    taskNameSet.add("mpitask3");

    Map<String, Double> ramMap = new HashMap<>();
    String ramMapStr = "mpitask1:5,mpitask2:6,mpitask3:7";

    if (ramMapStr != null) {
      String[] ramMapTokens = ramMapStr.split(",");
      for (String token : ramMapTokens) {
        if (token.trim().isEmpty()) {
          continue;
        }
        String[] taskAndRam = token.split(":");
        Double requiredRam = Double.parseDouble(taskAndRam[1]);
        ramMap.put(taskAndRam[0], requiredRam);
      }
    }
    return ramMap;
  }

  public static Map<String, Double> getTaskDiskMap(Job job) {

    Set<String> taskNameSet = new HashSet<String>();
    taskNameSet.add("mpitask1");
    taskNameSet.add("mpitask2");
    taskNameSet.add("mpitask3");

    Map<String, Double> diskMap = new HashMap<>();
    String diskMapStr = "mpitask1:5,mpitask2:6,mpitask3:7";

    if (diskMapStr != null) {
      String[] diskMapTokens = diskMapStr.split(",");
      for (String token : diskMapTokens) {
        if (token.trim().isEmpty()) {
          continue;
        }
        String[] taskAndDisk = token.split(":");
        Double requiredDisk = Double.parseDouble(taskAndDisk[1]);
        diskMap.put(taskAndDisk[0], requiredDisk);
      }
    }
    return diskMap;
  }

  public static Map<String, Double> getTaskCPUMap(Job job) {

    Set<String> taskNameSet = new HashSet<String>();
    taskNameSet.add("mpitask1");
    taskNameSet.add("mpitask2");
    taskNameSet.add("mpitask3");

    Map<String, Double> cpuMap = new HashMap<>();
    String cpuMapStr = "mpitask1:5,mpitask2:6,mpitask3:7";

    if (cpuMapStr != null) {
      String[] diskMapTokens = cpuMapStr.split(",");
      for (String token : diskMapTokens) {
        if (token.trim().isEmpty()) {
          continue;
        }
        String[] taskAndDisk = token.split(":");
        Double requiredDisk = Double.parseDouble(taskAndDisk[1]);
        cpuMap.put(taskAndDisk[0], requiredDisk);
      }
    }
    return cpuMap;
  }

  public static int getTotalNumberOfInstances(Job job) {
    HashMap<String, Integer> parallelTaskMap = getParallelTaskMap(job);
    int totalNumberOfInstances = 0;
    for (int instances : parallelTaskMap.values()) {
      totalNumberOfInstances += instances;
    }
    return totalNumberOfInstances;
  }

  public static HashMap<String, Integer> getParallelTaskMap(Job job) {
    HashMap<String, Integer> parallelTaskMap = new HashMap<>();
    int count = job.getTasklist().length;
    for (int i = 0; i < job.getTasklist().length; i++) {
      String taskName = job.getTasklist()[i].getTaskName();
      Integer parallelTaskCount = job.getTasklist()[i].getParallelTaskCount();
      parallelTaskMap.put(taskName, parallelTaskCount);
    }
    return parallelTaskMap;
  }

}
