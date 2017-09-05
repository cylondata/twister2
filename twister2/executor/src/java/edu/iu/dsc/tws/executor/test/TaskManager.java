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
package edu.iu.dsc.tws.executor.test;

import java.util.Date;

import edu.iu.dsc.tws.executor.api.observer.TaskHandler;
import edu.iu.dsc.tws.executor.model.Task;

public class TaskManager implements Runnable {

  private TaskHandler taskHandler;
  private String threadId;

  public TaskManager(TaskHandler taskHandler, String threadId) {
    this.taskHandler = taskHandler;
    this.threadId = threadId;
    System.out.println("Task Manager : Thread Id = "+threadId);
  }

  @Override
  public void run() {
    System.out.println("Running Task Manager : Thread Id = "+this.threadId);
    taskHandler.addTask(new Task(1000,"Task 1", new Date(), "Demo Task 1 to handle the queue",threadId));
    //taskHandler.addTask(new Task(1002,"Task 2", new Date(), "Demo Task 2 to handle the queue",threadId));
    //taskHandler.addTask(new Task(1003,"Task 3", new Date(), "Demo Task 3 to handle the queue",threadId));


  }
}
