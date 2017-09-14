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

import edu.iu.dsc.tws.tsched.utils.Job;

//This class will be replaced with the original JobAttributes file from the job package.

public class JobAttributes {

  private static final Logger LOG = Logger.getLogger(JobAttributes.class.getName());

  private static int numberOfContainers;
  private static int numberOfInstances;
  private static int totalNumberOfInstances;

  public static int getNumberOfContainers(Job job) {
    setNumberOfContainers();
    return numberOfContainers;
  }

  public static void setNumberOfContainers() {
    numberOfContainers = Integer.parseInt(JobConfig.Number_OF_Containers.trim());
  }

  public static int getNumberOfInstances(Job job) {
    return numberOfInstances;
  }

  public static void setNumberOfInstances() {
    numberOfInstances = Integer.parseInt(JobConfig.Number_OF_Instances.trim());
  }

  public static int getTotalNumberOfInstances(Job job) {
    HashMap<String,Integer> parallelTaskMap = getParallelTaskMap(job);
    int numberOfInstances = 0;
    for(int instances: parallelTaskMap.values()){
      numberOfInstances += instances;
    }
    return numberOfInstances;
  }

  public static HashMap<String, Integer> getParallelTaskMap(Job job) {
    HashMap<String, Integer> parallelTaskMap = new HashMap<>();
    int count = job.getTasklist().length;

    for(int i = 0; i < job.getTasklist().length; i++){
      String taskName = job.getTasklist()[i].getTaskName();
      Integer parallelTaskCount = job.getTasklist()[i].getParallelTaskCount();
      parallelTaskMap.put(taskName,parallelTaskCount);
      //System.out.println ("Task Details are:"+taskName+"\t"+parallelTaskCount);
    }
     return parallelTaskMap;
  }

  public static Map<String,Double> getComponentRAMMapConfig() {

    Set<String> componentNames = new HashSet<String>();
    componentNames.add("mapTask1");
    componentNames.add("reduceTask1");
    //componentNames.add("mapTask2");
    //componentNames.add("reduceTask2");

    Map<String, Double> RAMMap = new HashMap<>();
    String ramMapStr = "mapTask1:4,reduceTask1:4";

    if (ramMapStr != null) {
      String[] ramMapTokens = ramMapStr.split(",");
      for (String token : ramMapTokens) {
        if (token.trim().isEmpty()) {
          continue;
        }
        String[] componentAndRAM = token.split(":");
        Double requiredRAM = Double.parseDouble (componentAndRAM [1]);
        RAMMap.put(componentAndRAM [0], requiredRAM);
        System.out.println("Component And RAM.length:\t"+componentAndRAM .length);
      }
    }
    return RAMMap;
  }

  public static Map<String,Double> getComponentDiskMapConfig() {

    Set<String> componentNames = new HashSet<String>();
    componentNames.add("mapTask1");
    componentNames.add("reduceTask1");
    //componentNames.add("mapTask2");
    //componentNames.add("reduceTask2");

    Map<String, Double> DiskMap = new HashMap<>();
    String diskMapStr = "mapTask1:4,reduceTask1:4";

    if (diskMapStr != null) {
      String[] diskMapTokens = diskMapStr.split(",");
      for (String token : diskMapTokens) {
        if (token.trim().isEmpty()) {
          continue;
        }
        String[] componentAndDisk = token.split(":");
        Double requiredDisk = Double.parseDouble (componentAndDisk [1]);
        DiskMap.put(componentAndDisk [0], requiredDisk);
        System.out.println("Component And Disk.length:\t"+componentAndDisk .length);
      }
    }
    return DiskMap;
  }

  public static Map<String,Double> getComponentCPUMapConfig() {

    Set<String> componentNames = new HashSet<String>();
    componentNames.add("mapTask1");
    componentNames.add("reduceTask1");
    //componentNames.add("mapTask2");
    //componentNames.add("reduceTask2");

    Map<String, Double> CPUMap = new HashMap<>();
    String cpuMapStr = "mapTask1:4,reduceTask1:4";

    if (cpuMapStr != null) {
      String[] diskMapTokens = cpuMapStr.split(",");
      for (String token : diskMapTokens) {
        if (token.trim().isEmpty()) {
          continue;
        }
        String[] componentAndDisk = token.split(":");
        Double requiredDisk = Double.parseDouble (componentAndDisk[1]);
        CPUMap.put(componentAndDisk[0], requiredDisk);
        System.out.println("Component And Disk.length:\t"+componentAndDisk.length);
      }
    }
    return CPUMap;
  }

}
