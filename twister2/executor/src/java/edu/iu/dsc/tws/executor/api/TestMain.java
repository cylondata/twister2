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
package edu.iu.dsc.tws.executor.api;

import java.util.Date;

import edu.iu.dsc.tws.executor.model.Task;

/**
 * Created by vibhatha on 8/25/17.
 */
public class TestMain {
  public static void main(String[] args) {
    TaskHandler taskHandler = new TaskHandler();

    taskHandler.registerTaskAddedListener(new GetInfoTaskAddedListener());
    taskHandler.addTask(new Task(1001,"Task 1", new Date(), "Demo Task 1 to handle the queue"));
    taskHandler.addTask(new Task(1002,"Task 2", new Date(), "Demo Task 1 to handle the queue"));
    taskHandler.addTask(new Task(1003,"Task 3", new Date(), "Demo Task 1 to handle the queue"));


  }
}
