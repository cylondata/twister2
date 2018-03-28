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

//This class will be replaced with the original Job file from job package.

public class Job {

  public static int jobId = 0;
  public static Task[] taskList = null;
  public int taskLength = 0;
  public Job job = null;

  public Job getJob() {
    job = new Job();
    job.setJobId(1);
    job.setTaskLength(4);
    setJob(job);
    return job;
  }

  /**
   * This method is to set the task and its required processing values.
   */
  public void setJob(Job job) {

    //taskList = new Task[3];
    //job.setJobId(jobId);

    job.setJobId(job.getJobId());
    taskList = new Task[job.getTaskLength()];

    System.out.println("Task List Size Is:" + taskList.length + "\t"
        + "Corresponding Job Id:" + job.getJobId());

    /*Task t = new Task();
    t.setTaskName("mpitask1");
    t.setTaskCount(2);
    t.setRequiredRam(6.0);
    t.setRequiredDisk(20.0);
    t.setRequiredCpu(5.0);

    taskList[0] = t;

    Task t1 = new Task();
    t1.setTaskName("mpitask2");
    t1.setTaskCount(4);
    t1.setRequiredRam(6.0);
    t1.setRequiredDisk(20.0);
    t1.setRequiredCpu(5.0);

    taskList[1] = t1;

    Task t2 = new Task();
    t2.setTaskName("mpitask3");
    t2.setTaskCount(3);
    t2.setRequiredRam(6.0);
    t2.setRequiredDisk(20.0);
    t2.setRequiredCpu(5.0);

    taskList[2] = t2;

    Task t3 = new Task();
    t3.setTaskName("mpitask4");
    t3.setTaskCount(1);
    t3.setRequiredRam(6.0);
    t3.setRequiredDisk(20.0);
    t3.setRequiredCpu(5.0);

    taskList[3] = t3;

    job.setTasklist(taskList);*/

    this.job = job;
    //jobId++;
  }

  public int getTaskLength() {
    return taskLength;
  }

  public void setTaskLength(int taskLength) {
    this.taskLength = taskLength;
  }

  public Task[] getTasklist() {
    return taskList;
  }

  public void setTasklist(Task[] tasklist) {
    //this.taskList = tasklist;
    taskList = tasklist;
  }

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    Job.jobId = jobId;
  }

  public static class Task {

    public String taskName = "mpitask";
    public Integer taskCount = 0;
    public Double requiredRam = 0.0;
    public Double requiredDisk = 0.0;
    public Double requiredCpu = 0.0;

    public Double getRequiredRam() {
      return requiredRam;
    }

    public void setRequiredRam(Double requiredRam) {
      this.requiredRam = requiredRam;
    }

    public Double getRequiredDisk() {
      return requiredDisk;
    }

    public void setRequiredDisk(Double requiredDisk) {
      this.requiredDisk = requiredDisk;
    }

    public Double getRequiredCpu() {
      return requiredCpu;
    }

    public void setRequiredCpu(Double requiredCPU) {
      this.requiredCpu = requiredCPU;
    }

    public void setTaskCount(Integer taskCount) {
      this.taskCount = taskCount;
    }

    public String getTaskName() {
      return taskName;
    }

    public void setTaskName(String taskName) {
      this.taskName = taskName;
    }

    public Integer getParallelTaskCount() {
      return taskCount;
    }
  }
}


