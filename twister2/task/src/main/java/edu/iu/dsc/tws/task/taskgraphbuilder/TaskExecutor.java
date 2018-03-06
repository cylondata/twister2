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
package edu.iu.dsc.tws.task.taskgraphbuilder;

/**
 * This taskexecutor is generated for testing purpose, later
 * it will be replaced with actual task executors.
 */
public class TaskExecutor implements Runnable {

  public void execute(TaskGraphMapper task) {
    System.out.println("Runnable task for the executor:" + task);
    task.execute();
    //task.run();
  }

  @Override
  public void run() {
    System.out.println("hello execution finished");
  }

  public void print() {
    System.out.println("Just for testing");
  }
}
