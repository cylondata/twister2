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
package edu.iu.dsc.tws.task.api;

/**
 * Created by pulasthi on 11/8/17.
 */
public class RunnableTask implements Runnable {
  private Task executableTask;

  public RunnableTask(Task task) {
    this.executableTask = task;
  }
  public Task getExecutableTask() {
    return executableTask;
  }

  public void setExecutableTask(Task executableTask) {
    this.executableTask = executableTask;
  }

  @Override
  public void run() {
    if(executableTask == null){
      throw new RuntimeException("Task needs to be set to execute");
    }
    executableTask.execute();
  }
}
