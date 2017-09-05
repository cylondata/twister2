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

//This class will be replaced with the original JobAttributes file from the job package.

public class JobAttributes {

  private static final Logger LOG = Logger.getLogger(JobAttributes.class.getName());

  public static int numberOfContainers;
  public static int numberOfInstances;
  public static int totalNumberOfInstances;

  public static int getNumberOfContainers(Job job) {
    setNumberOfContainers();
    return numberOfContainers;
  }

  public static void setNumberOfContainers() {
    numberOfContainers = Integer.parseInt(JobConfig.Number_OF_Containers.trim());
  }

  /*public void setNumberOfContainers(int numberOfContainers) {
    //numberOfContainers = Integer.parseInt(JobConfig.Number_OF_Containers.trim());
    this.numberOfContainers = numberOfContainers;
  }*/

  public static int getNumberOfInstances(Job job) {
    return numberOfInstances;
  }

  public static void setNumberOfInstances() {
    numberOfInstances = Integer.parseInt(JobConfig.Number_OF_Instances.trim());
  }

  /*public void setNumberOfInstances(int numberOfInstances) {
    this.numberOfInstances = numberOfInstances;
  }*/

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

    //System.out.println("Job Id is:"+job.getJobId());
    //System.out.println("Task length is:"+count);

    for(int i = 0; i < job.getTasklist().length; i++){
      String taskName = job.getTasklist()[i].getTaskName();
      Integer parallelTaskCount = job.getTasklist()[i].getParallelTaskCount();
      parallelTaskMap.put(taskName,parallelTaskCount);
      //System.out.println ("Task Details are:"+taskName+"\t"+parallelTaskCount);
    }
    //System.out.println("Parallel Task Map Size:"+parallelTaskMap.size());
    return parallelTaskMap;
  }


}
