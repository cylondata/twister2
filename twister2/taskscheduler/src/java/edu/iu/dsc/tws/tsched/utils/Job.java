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

@SuppressWarnings("ALL")
public class Job {

  public static int jobId = 0;
  public Task[] tasklist = null;
  public Job job = null;

  public Job() {
  }

  public static void setJobId(int jobId) {
    Job.jobId = jobId;
  }

  public Job getJob() {

    Job job = new Job();
    setJob(job);
    return job;
  }

  public void setJob(Job job) {

    tasklist = new Task[2];

    job.setJobId(jobId);
    Task t = new Task();

    t.setTaskName("mpitask1");
    t.setTaskCount(2);

    tasklist[0] = t;

    Task t1 = new Task();
    t1.setTaskName("mpitask2");
    t1.setTaskCount(4);

    tasklist[1] = t1;

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

  public class Task {

    private String taskName = "mpitask";
    private Integer taskCount = 1;

    public void setTaskName(String taskName) {
      this.taskName = taskName;
    }

    public void setTaskCount(Integer taskCount) {
      this.taskCount = taskCount;
    }

    public String getTaskName() {
      return taskName;
    }

    public Integer getParallelTaskCount() {
      return taskCount;
    }

  }
}
