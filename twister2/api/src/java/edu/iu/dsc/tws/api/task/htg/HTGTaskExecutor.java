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
package edu.iu.dsc.tws.api.task.htg;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.api.task.TaskExecutor;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.worker.JobListener;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.master.worker.JMWorkerMessenger;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class HTGTaskExecutor extends TaskExecutor implements JobListener {
  private static final Logger LOG = Logger.getLogger(HTGTaskExecutor.class.getName());

  private BlockingQueue<Any> executeMessageQueue;

  private int workerId;

  public HTGTaskExecutor(Config cfg, int wId, List<JobMasterAPI.WorkerInfo> workerInfoList,
                         Communicator net) {
    super(cfg, wId, workerInfoList, net);
    this.executeMessageQueue = new LinkedBlockingQueue<>();
    this.workerId = wId;
  }

/**
 * execute*/
  public boolean execute(Map<String, DataFlowTaskGraph> dataFlowTaskGraphMap) {
    Any msg;
    HTGJobAPI.ExecuteMessage executeMessage;
    DataFlowTaskGraph taskGraph;
    ExecutionPlan executionPlan;
    HTGJobAPI.ExecuteCompletedMessage completedMessage;
    String subgraph;

    JMWorkerMessenger workerMessenger = JMWorkerAgent.getJMWorkerAgent().getJMWorkerMessenger();

    while (true) {
      try {
        msg = executeMessageQueue.take();

        if (msg.is(HTGJobAPI.ExecuteMessage.class)) {
          try {
            executeMessage = msg.unpack(HTGJobAPI.ExecuteMessage.class);
            LOG.log(Level.INFO, workerId + "Processing execute message: " + executeMessage);
            subgraph = executeMessage.getSubgraphName();
            LOG.log(Level.INFO, workerId + " Executing the subgraph : " + subgraph);

            // get the subgraph from the map
            taskGraph = dataFlowTaskGraphMap.get(executeMessage.getSubgraphName());
            if (taskGraph == null) {
              LOG.severe(workerId + " Unable to find the subgraph " + subgraph);
              return false;
            }
            // use the taskexecutor to create the execution plan
            executionPlan = this.plan(taskGraph);
            LOG.log(Level.INFO, workerId + " exec plan : " + executionPlan);
            LOG.log(Level.INFO, workerId + " exec plan : " + executionPlan.getNodes());

            // reuse the task executor execute
            this.execute(taskGraph, executionPlan);

            LOG.log(Level.INFO, workerId + " Completed subgraph : " + subgraph);

            LOG.log(Level.INFO, workerId + " Sending subgraph completed message to driver");
            completedMessage = HTGJobAPI.ExecuteCompletedMessage.newBuilder()
                .setSubgraphName(subgraph).build();

            if (!workerMessenger.sendToDriver(completedMessage)) {
              LOG.severe("Unable to send the subgraph completed message :" + completedMessage);
            }
          } catch (InvalidProtocolBufferException e) {
            LOG.log(Level.SEVERE, "Unable to unpack received message ", e);
          }
        } else if (msg.is(HTGJobAPI.HTGJobCompletedMessage.class)) {
          LOG.log(Level.INFO, workerId + "Received HTG job completed message. Leaving execution "
              + "loop");
          break;
        } else {
          LOG.log(Level.WARNING, workerId + "Unknown message for htg task execution");
        }
      } catch (InterruptedException e) {
        LOG.log(Level.SEVERE, "Unable to insert message to the queue", e);
      }
    }

    LOG.log(Level.INFO, workerId + " Execution Completed");
    return true;
  }

  @Override
  public void workersScaledUp(int instancesAdded) {
    LOG.log(Level.INFO, workerId + "Workers scaled up msg received. Instances added: "
        + instancesAdded);
  }

  @Override
  public void workersScaledDown(int instancesRemoved) {
    LOG.log(Level.INFO, workerId + "Workers scaled down msg received. Instances removed: "
        + instancesRemoved);
  }

  @Override
  public void broadcastReceived(Any anyMessage) {
    // put every message on the queue.
    try {
      this.executeMessageQueue.put(anyMessage);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Unable to insert message to the queue", e);
    }
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
    LOG.log(Level.INFO, workerId + "All workers joined msg received");
  }
}
