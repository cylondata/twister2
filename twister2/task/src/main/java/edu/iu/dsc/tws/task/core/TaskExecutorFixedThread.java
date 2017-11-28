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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import edu.iu.dsc.tws.task.api.Message;
import edu.iu.dsc.tws.task.api.Queue;
import edu.iu.dsc.tws.task.api.RunnableTask;
import edu.iu.dsc.tws.task.api.Task;
import edu.iu.dsc.tws.task.api.TaskMessage;

/**
 * Class that will handle the task execution. Each task executor will manage an task execution
 * thread pool that will be used to execute the tasks that are assigned to the particular task
 * executor.
 */
public class TaskExecutorFixedThread {

  /**
   * Thread pool used to execute tasks
   */
  private static ThreadPoolExecutor executorPool;
  /**
   * Hashmap that contains all the input and output queues of the executor and its associated
   * tasks
   */
  private Map<Integer, Queue<Message>> queues = new HashMap<Integer, Queue<Message>>();

  /**
   * Reverse mapping which maps each queue id to its input task
   */
  private Map<Integer, ArrayList<Integer>> queuexTaskInput =
      new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Reverse mapping which maps each queue id to its output task
   */
  private Map<Integer, ArrayList<Integer>> queuexTaskOutput =
      new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Hashmap that keeps the tasks and its input queues
   */
  private Map<Integer, ArrayList<Integer>> taskInputQueues =
      new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Hashmap that keeps the tasks and its output queues
   */
  private Map<Integer, ArrayList<Integer>> taskOutputQueues =
      new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Hashmap that contains the tasks that need to be executed
   */
  private Map<Integer, Task> taskMap = new HashMap<Integer, Task>();

  public TaskExecutorFixedThread() {
    initThreadPool(ExecutorContext.EXECUTOR_CORE_POOL_SIZE);
  }

  public TaskExecutorFixedThread(int corePoolSize) {
    initThreadPool(corePoolSize);
  }

  /**
   * Method used to register new queues
   */
  public <T> boolean registerTaskQueue(int qid, int taskId, ExecutorContext.QueueType type,
                                       Queue<Message> queue) {
    registerQueue(qid, queue);
    if (type == ExecutorContext.QueueType.INPUT) {
      if (!taskInputQueues.containsKey(taskId)) {
        taskInputQueues.put(taskId, new ArrayList<Integer>());
      }
      return taskInputQueues.get(taskId).add(qid);
    } else {
      if (!taskOutputQueues.containsKey(taskId)) {
        taskOutputQueues.put(taskId, new ArrayList<Integer>());
      }
      return taskOutputQueues.get(taskId).add(qid);
    }
  }

  public boolean registerTaskQueue(int qid, int taskId, ExecutorContext.QueueType type) {
    if (!queues.containsKey(qid)) {
      throw new RuntimeException(String.format("Cannot register queue : %d to task : %d."
          + " Queue %d is not registered", qid, taskId, qid));
    }
    if (type == ExecutorContext.QueueType.INPUT) {
      queuexTaskInput.get(qid).add(taskId);
      if (!taskInputQueues.containsKey(taskId)) {
        taskInputQueues.put(taskId, new ArrayList<Integer>());
      }
      return taskInputQueues.get(taskId).add(qid);
    } else {
      queuexTaskOutput.get(qid).add(taskId);
      if (!taskOutputQueues.containsKey(taskId)) {
        taskOutputQueues.put(taskId, new ArrayList<Integer>());
      }
      return taskOutputQueues.get(taskId).add(qid);
    }
  }

  public boolean registerQueue(int qid, Queue<Message> queue) {
    queues.put(qid, queue);
    queuexTaskInput.put(qid, new ArrayList<>());
    queuexTaskOutput.put(qid, new ArrayList<>());
    return true;
  }


  public boolean registerTask(int taskId, Task task, List<Integer> inputQueues,
                              List<Integer> outputQueues) {
    //Register task queues
    for (Integer inputQueue : inputQueues) {
      registerTaskQueue(inputQueue, taskId, ExecutorContext.QueueType.INPUT);
    }
    for (Integer outputQueue : outputQueues) {
      registerTaskQueue(outputQueue, taskId, ExecutorContext.QueueType.OUTPUT);
    }

    //If queues are registered add the task to task map
    taskMap.put(taskId, task);
    return true;
  }

  //TODO: Do we need to create a interface message that can be used as template for all messages?

  /**
   * Submit message to the given queue
   */
  public <T> boolean submitMessage(int qid, T message) {
    queues.get(qid).add(new TaskMessage<T>(message));

    //Add the related task to the execution queue
    for (Integer extaskid : queuexTaskInput.get(qid)) {
      executorPool.submit(new RunnableTask(taskMap.get(extaskid), queues.get(qid)));
    }

    //Add the related task to the execution queue
    for (Integer extaskid : queuexTaskInput.get(qid)) {
      executorPool.submit(new RunnableTask(taskMap.get(extaskid), queues.get(qid)));
    }
    return true;
  }

  /**
   * Init's the task thread pool
   */
  private void initThreadPool(int corePoolSize) {
    executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(corePoolSize);
  }
}
