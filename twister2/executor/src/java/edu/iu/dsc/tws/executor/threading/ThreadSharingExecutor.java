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

  private ExecutionPlan executionPlan;

  private boolean executorState = true;

  private boolean sourceExecutionDone = false;

  private boolean sinkExecutionDone = false;

  private boolean sourceCommunicationDone = false;

  private boolean sinkCommunicationDone = false;


  public ThreadSharingExecutor() {
  }

  public ThreadSharingExecutor(int numThreads) {
    this.numThreads = numThreads;
  }

  public ThreadSharingExecutor(ExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
  }

  @Override
  public void execute() {
    // go through the instances
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    for (INodeInstance node : tasks) {
      node.prepare();
    }

    for (int i = 0; i < tasks.size(); i++) {
      Thread t = new Thread(new Worker());
      t.setName("Thread-" + tasks.getClass().getSimpleName() + "-" + i);
      t.start();
      threads.add(t);
    }

    LOG.info("!@TaskSize = " + tasks.size());
  }

  private class Worker implements Runnable {
    private boolean exitCluase = (sourceExecutionDone && sourceCommunicationDone)
        && (sinkExecutionDone && sinkCommunicationDone);
    @Override
    public void run() {
      LOG.info("Exit Clause : " + exitCluase);
      while (!exitCluase) {
        INodeInstance nodeInstance = tasks.poll();

        if (nodeInstance != null) {

          if (nodeInstance instanceof SourceBatchInstance) {

            SourceBatchInstance sourceBatchInstance = (SourceBatchInstance) nodeInstance;
            sourceExecutionDone = sourceBatchInstance.execute();
            sourceCommunicationDone = sourceBatchInstance.communicationProgress();

          }

          /*if (sourceCommunicationDone && sourceExecutionDone) {
            LOG.info("Source Communication Done! " + ", SourceExecution : "
                + sourceExecutionDone + ", tasks : " + tasks.size());
          }*/

          if (nodeInstance instanceof SinkBatchInstance) {
            SinkBatchInstance sinkBatchInstance = (SinkBatchInstance) nodeInstance;
            sinkExecutionDone = sinkBatchInstance.execute();
            sinkCommunicationDone = sinkBatchInstance.commuinicationProgress();

            /*if (sinkCommunicationDone && sinkExecutionDone) {
              LOG.info("Sink Communication Done! " + ", SourceExecution : "
                  + sourceExecutionDone +  ", SinkExecution : " + sinkExecutionDone
                  + ", tasks : " + tasks.size());
            }*/
            //tasks.offer(nodeInstance);
          }

          /*LOG.info("SourceExecutionDone : " + sourceExecutionDone
              + ", sourceCommunicationDone : " + sourceCommunicationDone
              + ", sinkCommunicationDone : " + sinkCommunicationDone);*/
          //tasks.offer(nodeInstance);

          if (!(sourceExecutionDone && sourceCommunicationDone) || !(sinkExecutionDone
              && sinkCommunicationDone)) {
            tasks.offer(nodeInstance);
           /* LOG.info(String.format("SrcE : %s,  SrcC : %s, SnkE : %s, SnkC : %s",
                sourceExecutionDone, sourceCommunicationDone,
                sinkExecutionDone, sinkCommunicationDone));*/
          }

        }
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
