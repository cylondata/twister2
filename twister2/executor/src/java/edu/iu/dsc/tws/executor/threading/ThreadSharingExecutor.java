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
package edu.iu.dsc.tws.executor.threading;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.core.batch.SinkBatchInstance;
import edu.iu.dsc.tws.executor.core.batch.SourceBatchInstance;


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

  private boolean executorState = true;

  private boolean sourceExecutionDone = false;

  private boolean sinkExecutionDone = false;

  private boolean sourceCommunicationDone = false;

  private boolean sinkCommunicationDone = false;

  private boolean exitClause = false;

  private final Lock lock = new ReentrantLock();

  private int totalTasks = 0;

  private int finishedTaskNo = 0;

  private boolean isExecutionFinished = false;

  private HashMap<Integer, Boolean> taskList = new HashMap<>();

  private static int totalTaskCount;


  public ThreadSharingExecutor() {
  }

  public ThreadSharingExecutor(int numThreads) {
    this.numThreads = numThreads;
  }

  public ThreadSharingExecutor(ExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
    confinishedTasks = new ConcurrentHashMap<>();
  }

  // TODO : Create Separate SourceWorker and SinkWorker and run the node instances separately
  // TODO : Store the finished status and finish the execution and return the status of execution

  @Override
  public boolean execute() {
    // go through the instances
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());
    totalTasks = tasks.size();
    totalTaskCount += totalTasks;

    for (INodeInstance node : tasks) {
      node.prepare();
      if (node instanceof SourceBatchInstance) {
        taskList.put(((SourceBatchInstance) node).getBatchTaskId(), false);
      }

      if (node instanceof SinkBatchInstance) {
        taskList.put(((SinkBatchInstance) node).getBatchTaskId(), false);
      }
    }

    for (int i = 0; i < tasks.size(); i++) {
      Thread t = new Thread(new Worker());
      t.setName("Thread-From-ReduceBatchTask : " + i);
      t.start();
      threads.add(t);
    }


    // finishedTaskNo != totalTasks
   /* while (tasks.size() > 0) {
      //System.out.println("Task Progress : " + finishedTaskNo + "/" + totalTasks);
      INodeInstance iNodeInstance = tasks.poll();
      if (iNodeInstance != null) {
        if (iNodeInstance instanceof SourceBatchInstance) {
          SourceBatchInstance sourceBatchInstance = (SourceBatchInstance) iNodeInstance;
          Thread sourceWorkerThread
              = new Thread(new SourceWorker(sourceBatchInstance));
          sourceWorkerThread.start();
        }

        if (iNodeInstance instanceof SinkBatchInstance) {
          Thread sinkWorkerThread = new Thread(new SinkWorker((SinkBatchInstance) iNodeInstance));
          sinkWorkerThread.start();
        }

        tasks.offer(iNodeInstance);
      }
    }*/

    return isExecutionFinished;
  }

  protected class Worker implements Runnable {

    private int finishedTasks = 0;

    @Override
    public void run() {

      while (true) {

        INodeInstance nodeInstance = tasks.poll();

        if (nodeInstance != null) {
          if (nodeInstance instanceof SourceBatchInstance) {
            SourceBatchInstance sourceBatchInstance = (SourceBatchInstance) nodeInstance;
            sourceExecutionDone = sourceBatchInstance.execute();
            sourceCommunicationDone = sourceBatchInstance.communicationProgress();
            /*System.out.println("Src :" + nodeInstance + " Comms : " + sourceCommunicationDone
                + ", Exec : " + sourceExecutionDone);*/
            if (sourceExecutionDone && sourceCommunicationDone) {
              taskList.put(sourceBatchInstance.getBatchTaskId(), true);
              /*System.out.println("Src => Exec : " + sourceExecutionDone + ", Comms : "
                  + sourceCommunicationDone + ", Worker Id : " + sourceBatchInstance.getWorkerId()
                  + ", BatchTaskId : " + sourceBatchInstance.getBatchTaskId() + ", BT Index : "
                  + sourceBatchInstance.getBatchTaskIndex() + ", BatchTaskName : "
                  + sourceBatchInstance.getBatchTaskName());*/
            }
          }

          if (nodeInstance instanceof SinkBatchInstance) {
            SinkBatchInstance sinkBatchInstance = (SinkBatchInstance) nodeInstance;
            sinkExecutionDone = sinkBatchInstance.execute();
            sinkCommunicationDone = sinkBatchInstance.commuinicationProgress();
            /*System.out.println("Src :" + nodeInstance + " Comms : " + sourceCommunicationDone
                + ", Exec : " + sourceExecutionDone);*/
            if (sinkCommunicationDone && sinkExecutionDone) {
              taskList.put(sinkBatchInstance.getBatchTaskId(), true);
              /*System.out.println("Sink => Exec : " + sinkExecutionDone + ", Comms : "
                  + sinkCommunicationDone + ", Worker Id : " + sinkBatchInstance.getWorkerId()
                  + ", BatchTaskId : " + sinkBatchInstance.getBatchTaskId() + ", BT Index : "
                  + sinkBatchInstance.getBatchTaskIndex() + ", BatchTaskName : "
                  + sinkBatchInstance.getTaskName());*/
            }
          }
          int completedTasks = 0;
          for (Map.Entry<Integer, Boolean> e : taskList.entrySet()) {
            if (e.getValue()) {
              completedTasks++;
            } else {
              //
            }
          }

          if (completedTasks == taskList.size()) {
            isExecutionFinished = true;
            /*System.out.println("Tasks " + completedTasks + "/" + taskList.size()
                + "," + totalTasks);*/
          } else {
            tasks.offer(nodeInstance);
          }

        }



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
