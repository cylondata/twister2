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

import edu.iu.dsc.tws.task.api.Queue;
import edu.iu.dsc.tws.task.api.Task;

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
  private Map<Integer,Queue> queues = new HashMap<Integer, Queue>();

  /**
   * Reverse mapping which maps each queue id to its task
   */
  private Map<Integer,Integer> queuexTask = new HashMap<Integer, Integer>();

  /**
   * Hashmap that keeps the tasks and its input queues
   */
  private Map<Integer,ArrayList<Integer>> taskInputQueues = new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Hashmap that keeps the tasks and its output queues
   */
  private Map<Integer,ArrayList<Integer>> taskOutputQueues = new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Hashmap that contains the tasks that need to be executed
   */
  private Map<Integer, Task> taskMap = new HashMap<Integer, Task>();

  public TaskExecutorFixedThread(){
    initThreadPool(ExecutorContext.EXECUTOR_CORE_POOL_SIZE);
  }

  public TaskExecutorFixedThread(int corePoolSize){
    initThreadPool(corePoolSize);
  }

  /**
   * Method used to register new queues
   * @param qid
   * @param taskId
   * @param type
   * @param queue
   * @return
   */
  public boolean registerQueue(int qid,int taskId, ExecutorContext.QueueType type, Queue queue){
    queues.put(qid,queue);
    if(type == ExecutorContext.QueueType.INPUT){
      if(!taskInputQueues.containsKey(taskId)) taskInputQueues.put(taskId,new ArrayList<Integer>());
      return taskInputQueues.get(taskId).add(qid);
    }else{
      if(!taskOutputQueues.containsKey(taskId)) taskOutputQueues.put(taskId,new ArrayList<Integer>());
      return taskOutputQueues.get(taskId).add(qid);
    }
  }

  public boolean registerQueue(int qid,int taskId, ExecutorContext.QueueType type){
    if(!queues.containsKey(qid)) throw new RuntimeException(String.format("Cannot register queue : %d to task : %d. Queue %d is not registered", qid, taskId, qid));
    if(type == ExecutorContext.QueueType.INPUT){
      if(!taskInputQueues.containsKey(taskId)) taskInputQueues.put(taskId,new ArrayList<Integer>());
      return taskInputQueues.get(taskId).add(qid);
    }else{
      if(!taskOutputQueues.containsKey(taskId)) taskOutputQueues.put(taskId,new ArrayList<Integer>());
      return taskOutputQueues.get(taskId).add(qid);
    }
  }

  public boolean registerQueue(int qid, Queue queue){
    queues.put(qid,queue);
    return true;
  }


  public boolean registerTask(int taskId, Task task, List<Integer> inputQueues,List<Integer> outputQueues){
    taskMap.put(taskId, task);
    
    return true;
  }

  /**
   * Init's the task thread pool
   * @param corePoolSize
   */
  private void initThreadPool(int corePoolSize){
    executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(corePoolSize);
  }
}
