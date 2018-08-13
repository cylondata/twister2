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

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.api.ExecutionModel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.core.batch.SinkBatchInstance;
import edu.iu.dsc.tws.executor.core.batch.SourceBatchInstance;
import edu.iu.dsc.tws.task.graph.OperationMode;


public class ThreadExecutorFactory {

  private ExecutionModel executionModel;

  private ThreadExecutor executor;

  private ExecutionPlan executionPlan;

  private TWSChannel channel;

  private OperationMode operationMode;

  private boolean isExecutionFinished = false;

  private HashMap<Integer, Boolean> instanceBooleanHashMap;

  private HashMap<Integer, Boolean> sourceInstanceMap;

  private HashMap<Integer, Boolean> sinkInstanceMap;

  public ThreadExecutorFactory(ExecutionModel executionModels, ThreadExecutor executor,
                               ExecutionPlan executionPlan) {
    this.executionModel = executionModels;
    this.executor = executor;
    this.executionPlan = executionPlan;
    this.instanceBooleanHashMap = new HashMap<>();
    this.sourceInstanceMap = new HashMap<>();
    this.sinkInstanceMap = new HashMap<>();
  }

  public ThreadExecutorFactory(ExecutionModel executionModel, ThreadExecutor executor,
                               ExecutionPlan executionPlan, TWSChannel channel) {
    this.executionModel = executionModel;
    this.executor = executor;
    this.executionPlan = executionPlan;
    this.channel = channel;
  }

  public ThreadExecutorFactory(ExecutionModel executionModel, ThreadExecutor executor,
                               ExecutionPlan executionPlan, TWSChannel channel,
                               OperationMode operationMode) {
    this.executionModel = executionModel;
    this.executor = executor;
    this.executionPlan = executionPlan;
    this.channel = channel;
    this.operationMode = operationMode;
  }

  public boolean execute() {
    if (ExecutionModel.SHARING.equals(executionModel.getExecutionModel())) {
      ThreadSharingExecutor threadSharingExecutor
          = new ThreadSharingExecutor(executionPlan, channel, operationMode);
      isExecutionFinished = threadSharingExecutor.execute();
      return isExecutionFinished;
    } else if (ExecutionModel.DEDICATED.equals(executionModel.getExecutionModel())) {
      ThreadStaticExecutor threadStaticExecutor = new ThreadStaticExecutor(executionPlan);
      isExecutionFinished = threadStaticExecutor.execute();
      return isExecutionFinished;
    } else if (ExecutionModel.SHARED.equals(executionModel.getExecutionModel())) {
      ThreadSharedExecutor threadSharedExecutor
          = new ThreadSharedExecutor(executionModel, executor, executionPlan);
      isExecutionFinished = threadSharedExecutor.execute();
      return isExecutionFinished;
    } else {
      return false;
    }
  }

  public void initExecutionMap() {
    ExecutionPlan plan = this.executionPlan;
    for (Map.Entry<Integer, INodeInstance> e : plan.getNodes().entrySet()) {
      if (e.getValue() instanceof SourceBatchInstance) {
        int taskId = ((SourceBatchInstance) e.getValue()).getBatchTaskId();
        sourceInstanceMap.put(taskId, false);
      }

      if (e.getValue() instanceof SinkBatchInstance) {
        if (e.getValue() instanceof SinkBatchInstance) {
          int taskId = ((SinkBatchInstance) e.getValue()).getBatchTaskId();
          sinkInstanceMap.put(taskId, false);
        }
      }
    }
  }

  public void showExecutionMap() {
    System.out.println("Show Execution Map");
    if (sinkInstanceMap != null && sourceInstanceMap != null) {
      if (sourceInstanceMap.size() > 0) {
        System.out.println("Source Maps");
        for (Map.Entry<Integer, Boolean> e : sourceInstanceMap.entrySet()) {
          System.out.println(e.getKey() + ", " + e.getValue());
        }
      }
      if (sinkInstanceMap.size() > 0) {
        System.out.println("Sink Maps");
        for (Map.Entry<Integer, Boolean> e : sinkInstanceMap.entrySet()) {
          System.out.println(e.getKey() + ", " + e.getValue());
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
      while (!(this.executionDone && this.commmunicationDone)) {
        this.executionDone = this.instance.execute();
        this.commmunicationDone = this.instance.communicationProgress();
        if (this.executionDone && this.commmunicationDone) {
          sourceInstanceMap.put(this.instance.getBatchTaskId(), true);
        }
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
      while (!(this.executionDone && this.commmunicationDone)) {
        this.executionDone = this.instance.execute();
        this.commmunicationDone = this.instance.commuinicationProgress();
        if (this.commmunicationDone && this.executionDone) {
          sinkInstanceMap.put(this.instance.getBatchTaskId(), true);
        }
      }
    }
  }
}
