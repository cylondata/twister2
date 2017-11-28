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
  public Task[] tasklist = null;
  public Job job = null;
  //public String mapTask = null;
  //public String reduceTask = null;

  public Job getJob() {
    job = new Job();
    setJob(job);
    return job;
  }

  /*public String getMapTask() {
    return mapTask;
  }

  public void setMapTask(String mapTask, int i) {
    this.mapTask = mapTask;
  }

  public String getReduceTask() {
    return reduceTask;
  }

  public void setReduceTask(String reduceTask, int i) {
    this.reduceTask = reduceTask;
  }*/

  public void setJob(Job job) {

    tasklist = new Task[3];

    job.setJobId(jobId);
    Task t = new Task();

    t.setTaskName("mpitask1");
    t.setTaskCount(2);
    t.setRequiredRam(6.0);
    t.setRequiredDisk(20.0);
    t.setRequiredCpu(5.0);

    tasklist[0] = t;

    Task t1 = new Task();
    t1.setTaskName("mpitask2");
    t1.setTaskCount(4);
    t1.setRequiredRam(5.0);
    t1.setRequiredDisk(10.0);
    t1.setRequiredCpu(7.0);

    tasklist[1] = t1;

    Task t2 = new Task();
    t2.setTaskName("mpitask3");
    t2.setTaskCount(3);
    t2.setRequiredRam(7.0);
    t2.setRequiredDisk(15.0);
    t2.setRequiredCpu(10.0);

    tasklist[2] = t2;

    job.setTasklist(tasklist);

    this.job = job;
    jobId++;
  }

  public Task[] getTasklist() {
    return tasklist;
  }

  public void setTasklist(Task[] tasklist) {
    this.tasklist = tasklist;
  }

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    Job.jobId = jobId;
  }

  public class Task {

    private String taskName = "mpitask";
    private Integer taskCount = 1;
    private Double requiredRam = 0.0;
    private Double requiredDisk = 0.0;
    private Double requiredCpu = 0.0;

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
