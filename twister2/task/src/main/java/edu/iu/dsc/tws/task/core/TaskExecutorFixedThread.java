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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.Queue;
import edu.iu.dsc.tws.task.api.TaskExecutor;
import edu.iu.dsc.tws.task.api.TaskMessage;

/**
 * Class that will handle the task execution. Each task executor will manage an task execution
 * thread pool that will be used to execute the tasks that are assigned to the particular task
 * executor.
 */
public class TaskExecutorFixedThread implements TaskExecutor {

  private TWSChannel channel;
  private DataFlowOperation direct;
  private boolean progres = false;
  private static final Logger LOG = Logger.getLogger(TaskExecutorFixedThread.
      class.getName());

  /**
   * Thread pool used to execute tasks
   */
  public static ThreadPoolExecutor executorPool;

  /**
   * Message processing limit for a task
   */
  private int taskMessageProcessLimit = 1;
  /**
   * Hashmap that contains all the input and output queues of the executor and its associated
   * tasks
   */
  private Map<Integer, Queue<IMessage>> queues = new HashMap<Integer, Queue<IMessage>>();

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
  private Map<Integer, INode> taskMap = new HashMap<Integer, INode>();

  /**
   * Hashset that keeps track of all the tasks that are currently running and queued
   * in the thread pool
   */
  private static HashSet<Integer> submittedTasks = new HashSet<Integer>();

  public TaskExecutorFixedThread() {
    initExecutionPool(ExecutorContext.EXECUTOR_CORE_POOL_SIZE);
  }

  public TaskExecutorFixedThread(int corePoolSize) {
    initExecutionPool(corePoolSize);
  }

  /**
   * Method used to register new queues
   */
  public <T> boolean registerTaskQueue(int qid, int taskId, ExecutorContext.QueueType type,
                                       Queue<IMessage> queue) {
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

  public boolean registerQueue(int qid, Queue<IMessage> queue) {

    queues.put(qid, queue);
    queuexTaskInput.put(qid, new ArrayList<>());
    queuexTaskOutput.put(qid, new ArrayList<>());
    return true;
  }

  /**
   * Register sink tasks which only have input queues associated with the task
   */
  public boolean registerSinkTask(INode task, List<Integer> inputQueues) {
    return registerTask(task, inputQueues, inputQueues);
  }

  /**
   * Register source tasks which only have output queues associated with the task
   */
  public boolean registerSourceTask(INode task, List<Integer> outputQueues) {
    return registerTask(task, null, outputQueues);
  }

  /**
   * register tasks that does not contain any queues associated
   */
  public boolean registerTask(INode task) {
    return registerTask(task, new ArrayList<>(), new ArrayList<>());
  }

  /**
   * register a given task with executor
   */
  public boolean registerTask(INode task, List<Integer> inputQueues,
                              List<Integer> outputQueues) {
    LOG.info("------------------------------------------");
    LOG.info("Register Task");
    LOG.info("Task : " + 0);
    LOG.info("InputQueue : " + inputQueues.size());
    LOG.info("OutputQueue : " + outputQueues.size());
    LOG.info("------------------------------------------");
    //Register task queues
    //TODO: What happens in the queue already has data when task is registered
    if (inputQueues != null) {
      for (Integer inputQueue : inputQueues) {
        registerTaskQueue(inputQueue, 0, ExecutorContext.QueueType.INPUT);
      }
    }
    if (outputQueues != null) {
      for (Integer outputQueue : outputQueues) {
        registerTaskQueue(outputQueue, 0, ExecutorContext.QueueType.OUTPUT);
      }
    }

    //If queues are registered add the task to task map
    taskMap.put(0, task);
    return true;
  }

  //TODO: Do we need to create a interface message that can be used as template for all messages?

  /**
   * Submit message to the given queue
   */
  public synchronized <T> boolean submitMessage(int qid, T message) {
    //TODO; double check if the sync is correct and remove it if the method must by sync
    LOG.info("------------------------------------------------");
    LOG.info("QID : " + qid);
    LOG.info("Message : " + message.toString());
    LOG.info("------------------------------------------------");
    synchronized (ExecutorContext.FIXED_EXECUTOR_LOCK) {
      queues.get(qid).add(new TaskMessage(message));
    }
    //Add the related task to the execution queue
    for (Integer extaskid : queuexTaskInput.get(qid)) {
      if (!submittedTasks.contains(extaskid)) {
        addRunningTask(extaskid);
        executorPool.submit(new RunnableFixedTask(taskMap.get(extaskid), queues.get(qid),
            taskMessageProcessLimit, this, taskOutputQueues.get(extaskid)));
      }
    }

    //Add the related task to the execution queue
    for (Integer extaskid : queuexTaskOutput.get(qid)) {
      if (!submittedTasks.contains(extaskid)) {
        addRunningTask(extaskid);
        executorPool.submit(new RunnableFixedTask(taskMap.get(extaskid), queues.get(qid),
            taskMessageProcessLimit, this, taskOutputQueues.get(extaskid)));
      }
    }

    return true;
  }

  /**
   * Init's the task thread pool
   *
   * @param coreSize the size of the execution pool
   */
  @Override
  public void initExecutionPool(int coreSize) {
    executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(coreSize);
  }

  @Override
  public boolean submitTask(INode task) {
    if (!taskMap.containsKey(0)) {
      throw new RuntimeException(String.format("Unable to locate task with task id : %d, "
          + "Please make sure the task is registered", 0));
    } else {
      addRunningTask(0);
      executorPool.submit(new RunnableFixedTask(taskMap.get(0), this));
    }
    return true;
  }

  public boolean submitTask(int tid) {
    if (!taskMap.containsKey(tid)) {
      throw new RuntimeException(String.format("Unable to locate task with task id : %d, "
          + "Please make sure the task is registered", tid));
    } else {
      LOG.info("================================================");
      LOG.info("Submist Task : " + tid);
      LOG.info("================================================");
      addRunningTask(tid);
      executorPool.submit(new RunnableFixedTask(taskMap.get(tid), this));
    }
    return true;
  }

  @Override
  public Set<Integer> getRunningTasks() {
    return submittedTasks;
  }

  public static HashSet<Integer> getSubmittedTasks() {
    return submittedTasks;
  }

  @Override
  public boolean addRunningTasks(int tid) {
    return submittedTasks.add(tid);
  }

  @Override
  public boolean removeRunningTask(int tid) {
    return submittedTasks.remove(tid);
  }

  public static void setSubmittedTasks(HashSet<Integer> submittedTasks) {
    TaskExecutorFixedThread.submittedTasks = submittedTasks;
  }

  /**
   * Adds a task id to the runing tasks.Throws an RuntimeException if the task is already present
   *
   * @param tid the task id to be added
   */
  public static void addRunningTask(int tid) {
    if (submittedTasks.contains(tid)) {
      throw new RuntimeException(String.format("Trying to add already running task %d to the"
          + " running tasks set", tid));
    }
    submittedTasks.add(tid);
  }

  //TODO: might be able to remove sycn from this method need to confirm
  public static boolean removeSubmittedTask(int tid) {
    return submittedTasks.remove(tid);
  }

  /**
   * Init task executor
   */
  public void initCommunication(TWSChannel twscom, DataFlowOperation dfo) {
    this.channel = twscom;
    this.direct = dfo;
    this.progres = true;
  }

  public void progres() {
    while (progres) { //This can be done in a separate thread if that is more suitable
      channel.progress();
      direct.progress();
      Thread.yield();
    }
  }

  public void setProgress(boolean value) {
    this.progres = value;
  }

  public int getTaskMessageProcessLimit() {
    return taskMessageProcessLimit;
  }

  public void setTaskMessageProcessLimit(int taskMessageProcessLimit) {
    this.taskMessageProcessLimit = taskMessageProcessLimit;
  }
}
