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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.memory.MemoryManager;

public abstract class Task implements ITask {
  private static final long serialVersionUID = -252364900110286748L;
  /**
   * The unique id assigned to this task. This id will be used for communications
   */
  private int taskId;

  /**
   * Holds the memory manager associated with the task
   */
  private MemoryManager memoryManager;

  public Task() {

  }

  public Task(int tid) {
    this.taskId = tid;
  }

  public int taskId() {
    return taskId;
  }

  public MemoryManager getMemoryManager() {
    return memoryManager;
  }

  public void setMemoryManager(MemoryManager memoryManager) {
    this.memoryManager = memoryManager;
  }

  @Override
  public void prepare(Config cfg, OutputCollection collection) {

  }

  @Override
  public Message execute() {
    return null;
  }

  @Override
  public Message execute(Message content) {
    return null;
  }

  @Override
  public void run(Message content) {

  }

  @Override
  public void run() {

  }
}
