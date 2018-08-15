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
package edu.iu.dsc.tws.executor.threading;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.core.batch.SinkBatchInstance;
import edu.iu.dsc.tws.executor.core.batch.SourceBatchInstance;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class ThreadSharingExecutor extends ThreadExecutor {
  private static final Logger LOG = Logger.getLogger(ThreadSharingExecutor.class.getName());

  private int numThreads;

  private BlockingQueue<INodeInstance> tasks;

  private BlockingQueue<INodeInstance> executedTasks;

  private List<Thread> threads = new ArrayList<>();

  private HashMap<Thread, INodeInstance> threadTaskList = new HashMap<>();

  private final HashMap<INodeInstance, Boolean> finishedTasks = new HashMap<>();

  private Map<INodeInstance, Boolean> confinishedTasks = null;

  private ExecutionPlan executionPlan;

  private TWSChannel channel;

  private OperationMode operationMode;

  private boolean executorState = true;

  private AtomicBoolean sourceExecutionDone = new AtomicBoolean();

  private AtomicBoolean sinkExecutionDone = new AtomicBoolean();

  private AtomicBoolean sourceCommunicationDone = new AtomicBoolean();

  private AtomicBoolean sinkCommunicationDone = new AtomicBoolean();

  private final Lock lock = new ReentrantLock();

  private int totalTasks = 0;

  private AtomicBoolean isExecutionFinished = new AtomicBoolean();

  private HashMap<Integer, AtomicBoolean> taskList = new HashMap<>();

  private HashMap<INodeInstance, Integer> instanceTaskIdMap = new HashMap<>();

  private static int totalTaskCount;


  public ThreadSharingExecutor() {
  }

  public ThreadSharingExecutor(int numThreads) {
    this.numThreads = numThreads;
  }

  public ThreadSharingExecutor(ExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
    confinishedTasks = new ConcurrentHashMap<>();
    sourceCommunicationDone.set(false);
    sourceExecutionDone.set(false);
    sinkExecutionDone.set(false);
    sinkCommunicationDone.set(false);
  }

  public ThreadSharingExecutor(ExecutionPlan executionPlan, TWSChannel channel) {
    this.executionPlan = executionPlan;
    this.channel = channel;
    confinishedTasks = new ConcurrentHashMap<>();
    sourceCommunicationDone.set(false);
    sourceExecutionDone.set(false);
    sinkExecutionDone.set(false);
    sinkCommunicationDone.set(false);
    isExecutionFinished.set(false);
  }

  public ThreadSharingExecutor(ExecutionPlan executionPlan, TWSChannel channel,
                               OperationMode operationMode) {
    this.executionPlan = executionPlan;
    this.channel = channel;
    this.operationMode = operationMode;
  }

  // TODO : Create Separate SourceWorker and SinkWorker and run the node instances separately
  // TODO : Store the finished status and finish the execution and return the status of execution

  @Override
  public boolean execute() {
    // go through the instances
    System.out.println("Operation Mode : " + this.operationMode);
    if (this.operationMode == OperationMode.BATCH) {
      return batchExecute();
    }

    if (this.operationMode == OperationMode.STREAMING) {
      return streamExecute();
    }

    return false;
  }

  public boolean streamExecute() {

    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    for (INodeInstance node : tasks) {
      node.prepare();
    }

    for (int i = 0; i < tasks.size(); i++) {
      Thread t = new Thread(new StreamWorker());
      t.setName("Thread-" + tasks.getClass().getSimpleName() + "-" + i);
      t.start();
      threads.add(t);
    }

    progressStreamComm();

    return true;
  }

  /**
   * Execution Method for Batch Tasks
   * */
  public boolean batchExecute() {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());
    totalTasks = tasks.size();
    totalTaskCount += totalTasks;

    for (INodeInstance node : tasks) {
      node.prepare();
      AtomicBoolean initBool = new AtomicBoolean();
      initBool.set(false);

      if (node instanceof SourceBatchInstance) {
        int taskId = ((SourceBatchInstance) node).getBatchTaskId();
        taskList.put(taskId, initBool);
        instanceTaskIdMap.put(node, taskId);
      }

      if (node instanceof SinkBatchInstance) {
        int taskId = ((SinkBatchInstance) node).getBatchTaskId();
        taskList.put(taskId, initBool);
        instanceTaskIdMap.put(node, taskId);
      }

    }

    int curTaskSize = tasks.size();
    BatchWorker[] workers = new BatchWorker[curTaskSize];

    for (int i = 0; i < curTaskSize; i++) {
      workers[i] = new BatchWorker();
      Thread t = new Thread(workers[i]);
      t.setName("Thread-From-ReduceBatchTask : " + i);
      t.start();
      threads.add(t);
    }

    progressBatchComm();

    return isDone();
  }



  public void progressBatchComm() {
    while (!isDone()) {
      this.channel.progress();
    }
  }

  public void progressStreamComm() {
    while (true) {
      this.channel.progress();
    }
  }

  public synchronized boolean isDone() {
    boolean isDone = false;
    lock.lock();
    try {
      isDone = sinkCommunicationDone.get()
          && sinkExecutionDone.get()
          && sourceExecutionDone.get()
          && sourceCommunicationDone.get()
          && isExecutionFinished.get();
    } finally {
      lock.unlock();
    }

    return isDone;
  }


  protected class BatchWorker implements Runnable {

    private int finishedTasks = 0;

    private AtomicBoolean currentThreadStatus = new AtomicBoolean();
    private Object object = null;

    @Override
    public void run() {

      while (true) {

        INodeInstance nodeInstance = tasks.poll();

        if (nodeInstance != null) {
          if (nodeInstance instanceof SourceBatchInstance) {
            SourceBatchInstance sourceBatchInstance = (SourceBatchInstance) nodeInstance;
            sourceExecutionDone.set(sourceBatchInstance.execute());
            sourceCommunicationDone.set(sourceBatchInstance.communicationProgress());

            if (sourceExecutionDone.get() && sourceCommunicationDone.get()) {
              AtomicBoolean condition = new AtomicBoolean();
              condition.set(true);
              taskList.put(sourceBatchInstance.getBatchTaskId(), condition);
            }
            tasks.offer(sourceBatchInstance);
          }

          if (nodeInstance instanceof SinkBatchInstance) {
            SinkBatchInstance sinkBatchInstance = (SinkBatchInstance) nodeInstance;
            sinkExecutionDone.set(sinkBatchInstance.execute());
            sinkCommunicationDone.set(sinkBatchInstance.commuinicationProgress());

            if (sinkCommunicationDone.get() && sinkExecutionDone.get()) {
              AtomicBoolean condition = new AtomicBoolean();
              condition.set(true);
              taskList.put(sinkBatchInstance.getBatchTaskId(), condition);
            }
            tasks.offer(sinkBatchInstance);
          }

          boolean allDone = true;
          for (Map.Entry<Integer, AtomicBoolean> e : taskList.entrySet()) {
            //System.out.println("Task : " + e.getKey() + ", Value : " + e.getValue().get());
            if (!e.getValue().get()) {
              allDone = false;
            }
          }
          isExecutionFinished.set(allDone);
        }
      }
    }

    public synchronized AtomicBoolean getCurrentThreadStatus() throws InterruptedException {
      System.out.println("Getting current status from thread ...");
      while (object == null) {
        wait();
      }
      System.out.println("Got the current status from thread");
      return currentThreadStatus;
    }
  }

  protected class StreamWorker implements Runnable {

    @Override
    public void run() {
      while (true) {
        INodeInstance nodeInstance = tasks.poll();
        nodeInstance.execute();
        tasks.offer(nodeInstance);
      }
    }
  }


  protected class SourceWorker implements Runnable {

    private SourceBatchInstance instance;
    private boolean executionDone = false;
    private boolean commmunicationDone = false;

    public SourceWorker(SourceBatchInstance sourceBatchInstance) {
      this.instance = sourceBatchInstance;
    }

    @Override
    public void run() {
      this.executionDone = this.instance.execute();
      this.commmunicationDone = this.instance.communicationProgress();
      if (this.executionDone && this.commmunicationDone) {
        System.out.println("Src => Exec : " + this.executionDone + ", Comms : "
            + this.commmunicationDone + ", Worker Id : " + this.instance.getWorkerId()
            + ", BatchTaskId : " + this.instance.getBatchTaskId() + ", BT Index : "
            + this.instance.getBatchTaskIndex() + ", BatchTaskName : "
            + this.instance.getBatchTaskName());
        //System.out.println("Source " + this.instance + ", IsFinished : " + true);
        confinishedTasks.put(this.instance, true);
      }
    }
  }

  protected class SinkWorker implements Runnable {

    private SinkBatchInstance instance;
    private boolean executionDone = false;
    private boolean commmunicationDone = false;

    public SinkWorker(SinkBatchInstance sinkBatchInstance) {
      this.instance = sinkBatchInstance;
    }

    @Override
    public void run() {
      this.executionDone = this.instance.execute();
      this.commmunicationDone = this.instance.commuinicationProgress();
      if (this.commmunicationDone) {
        System.out.println("Sink => Exec : " + this.executionDone + ", Comms : "
            + this.commmunicationDone + ", Worker Id : " + this.instance.getWorkerId()
            + ", BatchTaskId : " + this.instance.getBatchTaskId() + ", BT Index : "
            + this.instance.getBatchTaskIndex() + ", BatchTaskName : "
            + this.instance.getTaskName());
        confinishedTasks.put(this.instance, true);
        //System.out.println("Sink " + this.instance + ", IsFinished : " + true);
      }
    }
  }

  public boolean isRunnable(INodeInstance iNodeInstance) {
    boolean status = true;
    if (iNodeInstance != null) {
      if (iNodeInstance instanceof SourceBatchInstance) {
        SourceBatchInstance sourceBatchInstance = (SourceBatchInstance) iNodeInstance;
        status = sourceBatchInstance.isFinish();
      }

      if (iNodeInstance instanceof SinkBatchInstance) {
        //
      }
    }

    return status;
  }
}
