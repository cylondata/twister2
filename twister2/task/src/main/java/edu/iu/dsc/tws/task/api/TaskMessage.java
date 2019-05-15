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
 * Wrapper interface for all the messages types.
 */
public class TaskMessage<T> implements IMessage<T> {
  /**
   * Stores the data
   */
  private T content;

  /**
   * The edge
   */
  private String edge;

  /**
   * Source task
   */
  private int sourceTask;

  private int flag;

  /**
   * Create a task message with data
   *
   * @param data data
   */
  public TaskMessage(T data) {
    this.content = data;
  }

  /**
   * Create a task message with data
   *
   * @param content data
   * @param edge edge
   * @param sourceTask sourcetask
   */
  public TaskMessage(T content, String edge, int sourceTask) {
    this.content = content;
    this.edge = edge;
    this.sourceTask = sourceTask;
  }

  public TaskMessage(T content, int flag, String edge, int sourceTask) {
    this(content, edge, sourceTask);
    this.flag = flag;
  }

  public T getContent() {
    return content;
  }

  public void setContent(T content) {
    this.content = content;
  }

  @Override
  public String edge() {
    return edge;
  }

  @Override
  public int getFlag() {
    return this.flag;
  }

  public int sourceTask() {
    return sourceTask;
  }

  @Override
  public void setFlag(int flag) {
    this.flag = flag;
  }
}
