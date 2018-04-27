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
public class TaskMessage implements IMessage {

  /**
   * Stores the data
   */
  private Object content;

  private String edge;

  private int sourceTask;

  private int targetTask;

  public TaskMessage(Object data) {
    this.content = data;
  }

  public TaskMessage(Object content, String edge, int sourceTask) {
    this.content = content;
    this.edge = edge;
    this.sourceTask = sourceTask;
  }

  public Object getContent() {
    return content;
  }

  public void setContent(Object content) {
    this.content = content;
  }

  @Override
  public String edge() {
    return edge;
  }

  public int sourceTask() {
    return sourceTask;
  }

  public int getTargetTask() {
    return targetTask;
  }
}
