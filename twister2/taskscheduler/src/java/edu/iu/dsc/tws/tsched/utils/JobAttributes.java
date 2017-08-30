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

public class JobAttributes {

  public static int numberOfContainers;
  public static int numberOfInstances;
  public static int totalNumberOfInstances;

  public static int getNumberOfContainers(Job job) {
    numberOfContainers = job.Number_Of_Containers;
    return numberOfContainers;
  }

  public void setNumberOfContainers(int numberOfContainers) {
    JobAttributes.numberOfContainers = numberOfContainers;
  }

  public static int getNumberOfInstances(Job job) {
    numberOfInstances = job.Number_Of_Instances;
    return numberOfInstances;
  }

  public void setNumberOfInstances(int numberOfInstances) {
    JobAttributes.numberOfInstances = numberOfInstances;
  }

  public static int getTotalNumberOfInstances(Job job) {
   HashMap<String,Integer> parallelTaskMap = getParallelTaskMap(job);
   int numberOfInstances = 0;
   for(int instances:parallelTaskMap.values()){
      numberOfInstances += instances;
   }
   return numberOfInstances;
  }

  public static HashMap<String, Integer> getParallelTaskMap(Job job) {
    HashMap<String, Integer> parallelTaskMap = new HashMap<>();
    for(Job.Task task: job.getTaskList()){
      String taskName = task.getTaskName();
      Integer parallelTaskCount = task.getParallelTaskCount();
      parallelTaskMap.put(taskName,parallelTaskCount);
    }
    return parallelTaskMap;
  }

}
