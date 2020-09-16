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

import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.ExecutorContext;
import edu.iu.dsc.tws.api.compute.executor.IExecutionHook;
import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.util.CommonThreadPool;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.executor.threading.ft.AllSharingBatchExecutor;
import edu.iu.dsc.tws.executor.threading.ft.AllSharingStremingExecutor;
import edu.iu.dsc.tws.executor.threading.ft.DedicatedComStreamingExecutor;
import edu.iu.dsc.tws.executor.threading.ft.DedidatedBatchExecutor;

public class ExecutorFactory {
  /**
   * Base configuration we are starting with
   */
  private Config config;

  /**
   * THe worker id
   */
  private int workerId;

  /**
   * The communication channel
   */
  private TWSChannel channel;


  public ExecutorFactory(Config cfg, int wId, TWSChannel ch) {
    this.config = cfg;
    this.workerId = wId;
    this.channel = ch;

    // initialize common thread pool
    if (!CommonThreadPool.isActive()) {
      CommonThreadPool.init(config);
    }
  }

  private IExecutor getExecutor(Config planConfig, OperationMode operationMode,
                                ExecutionPlan plan, IExecutionHook hook) {
    IExecutor executor;
    // if checkpointing enabled lets register for receiving faults
    if (CheckpointingContext.isCheckpointingEnabled(planConfig)) {
      // lets start the execution
      if (operationMode == OperationMode.STREAMING) {
        String streamExecutor = ExecutorContext.getStreamExecutor(planConfig);
        if (ExecutorContext.STREAM_EXECUTOR_ALL_SHARING.equals(streamExecutor)) {
          executor = new StreamingAllSharingExecutor(planConfig, workerId, channel, plan, hook);
        } else if (ExecutorContext.STREAM_EXECUTOR_DEDICATED_COMM.equals(streamExecutor)) {
          executor = new StreamingSharingExecutor(planConfig, workerId, channel, plan, hook);
        } else {
          throw new Twister2RuntimeException("Un-known stream executor specified - "
              + streamExecutor);
        }
      } else {
        String batchExecutor = ExecutorContext.getBatchExecutor(planConfig);
        if (ExecutorContext.BATCH_EXECUTOR_SHARING_SEP_COMM.equals(batchExecutor)) {
          executor = new BatchSharingExecutor(planConfig, workerId, channel, plan, hook);
        } else if (ExecutorContext.BATCH_EXECUTOR_SHARING.equals(batchExecutor)) {
          executor = new BatchSharingExecutor2(planConfig, workerId, channel, plan, hook);
        } else {
          throw new Twister2RuntimeException("Un-known batch executor specified - "
              + batchExecutor);
        }
      }
    } else {
      // lets start the execution
      if (operationMode == OperationMode.STREAMING) {
        String streamExecutor = ExecutorContext.getStreamExecutor(planConfig);
        if (ExecutorContext.STREAM_EXECUTOR_ALL_SHARING.equals(streamExecutor)) {
          executor = new AllSharingStremingExecutor(planConfig, workerId, channel, plan, hook);
        } else if (ExecutorContext.STREAM_EXECUTOR_DEDICATED_COMM.equals(streamExecutor)) {
          executor = new DedicatedComStreamingExecutor(planConfig, workerId, channel, plan, hook);
        } else {
          throw new Twister2RuntimeException("Un-known stream executor specified - "
              + streamExecutor);
        }
      } else {
        String batchExecutor = ExecutorContext.getBatchExecutor(planConfig);
        if (ExecutorContext.BATCH_EXECUTOR_SHARING_SEP_COMM.equals(batchExecutor)) {
          executor = new DedidatedBatchExecutor(planConfig, workerId, channel, plan, hook);
        } else if (ExecutorContext.BATCH_EXECUTOR_SHARING.equals(batchExecutor)) {
          executor = new AllSharingBatchExecutor(planConfig, workerId, channel, plan, hook);
        } else {
          throw new Twister2RuntimeException("Un-known batch executor specified - "
              + batchExecutor);
        }
      }
    }
    return executor;
  }

  /**
   * Get the executor
   * @param planConfig the plan specific configurations
   * @param executionPlan the execut
   * @param mode the operation mode
   * @return the executor
   */
  public IExecutor getExecutor(Config planConfig, ExecutionPlan executionPlan, OperationMode mode) {
    return getExecutor(planConfig, mode, executionPlan,
        new IExecutionHook() {
          @Override
          public void beforeExecution() {
          }

          @Override
          public void afterExecution() {
          }

          @Override
          public void onClose(IExecutor ex) {

          }
        });
  }

  /**
   * Get the executor
   * @param planConfig the plan specific configurations
   * @param executionPlan the execut
   * @param mode the operation mode
   * @return the executor
   */
  public IExecutor getExecutor(Config planConfig, ExecutionPlan executionPlan, OperationMode mode,
                               IExecutionHook hook) {
    return getExecutor(planConfig, mode, executionPlan, hook);
  }
}
