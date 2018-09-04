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

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.Queue;

/**
 * Wrapper class that is used to execute Tasks ( this is for the chached thread executor which is
 * not in use at the moment)
 */
public class RunnableTask implements Runnable {
  private INode executableTask;
  private Queue<IMessage> queueRef;
  private boolean isMessageBased = false;
  private int messageProcessLimit = 1;
  private int messageProcessCount = 0;

  public RunnableTask(INode task) {
    this.executableTask = task;
  }

  public RunnableTask(INode task, int messageLimit) {
    this.executableTask = task;
  }

  //TODO: would it better to send a referance to the queue and then use that to get the message?
  public RunnableTask(INode task, Queue<IMessage> msg) {
    this.executableTask = task;
    this.queueRef = msg;
    isMessageBased = true;
  }

  public RunnableTask(INode task, Queue<IMessage> msg, int messageLimit) {
    this.executableTask = task;
    this.queueRef = msg;
    this.messageProcessLimit = messageLimit;
    isMessageBased = true;
  }

  public INode getExecutableTask() {
    return executableTask;
  }

  public void setExecutableTask(INode executableTask) {
    this.executableTask = executableTask;
  }

  public int getMessageProcessCount() {
    return messageProcessCount;
  }

  public void setMessageProcessCount(int messageProcessCount) {
    this.messageProcessCount = messageProcessCount;
  }

  public boolean isMessageBased() {
    return isMessageBased;
  }

  public void setMessageBased(boolean messageBased) {
    isMessageBased = messageBased;
  }

  public int getMessageProcessLimit() {
    return messageProcessLimit;
  }

  public void setMessageProcessLimit(int messageProcessLimit) {
    this.messageProcessLimit = messageProcessLimit;
  }

  public Queue<IMessage> getQueueRef() {
    return queueRef;
  }

  public void setQueueRef(Queue<IMessage> queueRef) {
    this.queueRef = queueRef;
  }

  @Override
  public void run() {
    if (executableTask == null) {
      throw new RuntimeException("Task needs to be set to execute");
    }
    if (isMessageBased) {
      //TODO: check if this part needs to be synced
      while (!queueRef.isEmpty()) {
        if (messageProcessCount < messageProcessLimit) {
          messageProcessCount++;
        }
      }
    }
  }
}
