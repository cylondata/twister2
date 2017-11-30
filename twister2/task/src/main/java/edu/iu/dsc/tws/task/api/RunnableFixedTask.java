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
package edu.iu.dsc.tws.task.api;

import java.util.logging.Logger;

import edu.iu.dsc.tws.task.core.ExecutorContext;
import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;

/**
 * Wrapper class that is used to execute Tasks. This runnable task is to be used with the
 * TaskExecutorFixedThread
 */
public class RunnableFixedTask implements Runnable {

  private static final Logger LOG = Logger.getLogger(RunnableFixedTask.class.getName());

  private Task executableTask;
  private Queue<Message> queueRef;
  private boolean isMessageBased = false;
  private int messageProcessLimit = 1;
  private int messageProcessCount = 0;

  public RunnableFixedTask(Task task) {
    this.executableTask = task;
  }

  public RunnableFixedTask(Task task, int messageLimit) {
    this.executableTask = task;
  }

  //TODO: would it better to send a referance to the queue and then use that to get the message?
  public RunnableFixedTask(Task task, Queue<Message> msg) {
    this.executableTask = task;
    this.queueRef = msg;
    isMessageBased = true;
  }

  public RunnableFixedTask(Task task, Queue<Message> msg, int messageLimit) {
    this.executableTask = task;
    this.queueRef = msg;
    this.messageProcessLimit = messageLimit;
    isMessageBased = true;
  }

  public Task getExecutableTask() {
    return executableTask;
  }

  public void setExecutableTask(Task executableTask) {
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

  public Queue<Message> getQueueRef() {
    return queueRef;
  }

  public void setQueueRef(Queue<Message> queueRef) {
    this.queueRef = queueRef;
  }

  @Override
  public void run() {m
    if (executableTask == null) {
      throw new RuntimeException("Task needs to be set to execute");
    }
    LOG.info(String.format("Runnable task %d limit %d", executableTask.getTaskId(),
        messageProcessLimit));

    if (isMessageBased) {
      //TODO: check if this part needs to be synced
      while (!queueRef.isEmpty()) {
        if (messageProcessCount < messageProcessLimit) {
          executableTask.execute(queueRef.poll());
          messageProcessCount++;
        } else {
          //Need to make sure the remaining tasks are processed
          LOG.info("Need to run more so resubmitting the task");
          TaskExecutorFixedThread.executorPool.submit(
              new RunnableFixedTask(executableTask, queueRef, messageProcessLimit));
          return;
        }
      }
      synchronized (ExecutorContext.FIXED_EXECUTOR_LOCK) {
        if (!queueRef.isEmpty()) {
          LOG.info("Need to run more so resubmitting the task");
          TaskExecutorFixedThread.executorPool.submit(
              new RunnableFixedTask(executableTask, queueRef, messageProcessLimit));
        } else {
          TaskExecutorFixedThread.removeRunningTask(executableTask.getTaskId());
        }
      }
    } else {
      executableTask.execute();
      TaskExecutorFixedThread.removeRunningTask(executableTask.getTaskId());
    }
  }
}
