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
package edu.iu.dsc.tws.task.core;

import java.util.List;

import edu.iu.dsc.tws.task.api.Message;
import edu.iu.dsc.tws.task.api.Task;

/**
 * Class used to define pipelined tasks. A  pipelined task may hold 1 or more tasks that can be
 * executed in a pipelined manner. The output of the previous task will be directly used to execute
 * the next task in the pipline without having to submit messages to the queue.
 */
public class PipelinedTask extends Task {

  private List<Task> piplinedTaskList;

  public PipelinedTask(int tid, List<Task> taskList) {
    super(tid);
    if (taskList == null || taskList.isEmpty()) {
      throw new RuntimeException("PipelinedTask cannot be initiated with a null or "
          + "empty task list");
    }
    piplinedTaskList = taskList;
  }

  @Override
  public Message execute() {
    return executePipeline();
  }

  @Override
  public Message execute(Message content) {
    return executePipeline(content);
  }


  /**
   * Executes the list of tasks in order
   *
   * @return the result returned from the last task in the task list
   */
  private Message executePipeline() {
    return executePipeline(null);
  }

  /**
   * Executes the list of tasks in order
   *
   * @return the result returned from the last task in the task list
   */
  private Message executePipeline(Message content) {
    Message intermediateResult = content;
    for (Task task : piplinedTaskList) {
      if (intermediateResult == null) {
        intermediateResult = task.execute();
      } else {
        intermediateResult = task.execute(intermediateResult);
      }
    }
    return intermediateResult;
  }

  public List<Task> getPiplinedTaskList() {
    return piplinedTaskList;
  }

  public void setPiplinedTaskList(List<Task> piplinedTaskList) {
    this.piplinedTaskList = piplinedTaskList;
  }
}
