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
package edu.iu.dsc.tws.task.tsystem;

public class Task implements Runnable, TaskInterface {

  private final String taskId;
  private final Thread executionThread;
  private final TaskInformation taskInformation;
  private final TaskConfiguration taskConfiguration;

  /*private final Collection<String> taskRequiredJarFiles;
  private final Collection<String> taskRequiredInputFiles;
  private final Collection<String> taskClassPaths;
  private final JobId jobId;
  private final ExecutorID executorId;

  public Task(String taskId, TaskInformation taskInformation,
                TaskConfiguration taskConfiguration, Collection<String> requiredJarFiles,
                Collection<String> requiredInputFiles, Collection<String> requiredClasspaths) {
        this.taskId = taskId;
        this.taskInformation = taskInformation;
        this.taskConfiguration = taskConfiguration;
        this.taskRequiredJarFiles = requiredJarFiles;
        this.taskRequiredInputFiles = requiredInputFiles;
        executionThread = new Thread("Twister2 Task Thread");
  }*/

  public Task(String taskId, String threadName,
              TaskInformation taskInformation, TaskConfiguration taskConfiguration) {
    this.taskId = taskId;
    this.executionThread = new Thread(threadName);
    this.taskInformation = taskInformation;
    this.taskConfiguration = taskConfiguration;
  }

  /*public Collection<String> getTaskRequiredJarFiles() {
    return taskRequiredJarFiles;
  }

  public Collection<String> getTaskRequiredInputFiles() {
    return taskRequiredInputFiles;
  }*/

  public static void main(String[] args) {

    Task task = new Task("1", "execution thread",
        new TaskInformation(), new TaskConfiguration());
    Thread t1 = new Thread(task);
    t1.start();
  }

  public TaskInformation getTaskInformation() {
    return taskInformation;
  }

  public TaskConfiguration getTaskConfiguration() {
    return taskConfiguration;
  }

  public String getTaskId() {
    return taskId;
  }

  public Thread getExecutionThread() {
    return executionThread;
  }

  @Override
  public void run() {
    while (true) {
      System.out.println("I am executing my task thread");
    }
  }

  /**
   * Implement this method for actual execution
   **/
  public void execute() {
    executionThread.start();
  }

  /**
   * This method should be invoked when the task is ready for execution and it is not running previously
   **/
  // check the other conditions to
  private void dispatcher(Runnable runnable) {
    synchronized (this) {
      //create an executor object and submit it to the executor service
      try {
        //executor(task);
        System.out.println("Task Submitted to Executor");
      } catch (Exception ee) {
        ee.printStackTrace();
      }
    }
  }

  @Override
  public void cancel() {
  }

  public void createTask() {
  }
}

