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

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.task.api.Task;

/**
 * This class will be replaced with the original Executor code
 **/
public class Executor implements Runnable {

  public void execute(TaskMapper task, Class<CManager> cManager) {
    System.out.println("Runnable task for the executor:" + task);
    task.run();
  }

  public void execute(Task task, Class<DataFlowOperation> dataFlowOperationClass) {
    //task.run();
    System.out.println("Receiving the task:" + task);
  }

  public void execute(TaskMapper taskMapper) {
    System.out.println("Just for testing");
  }

  @Override
  public void run() {
    System.out.println("hello execution finished");
  }
}
