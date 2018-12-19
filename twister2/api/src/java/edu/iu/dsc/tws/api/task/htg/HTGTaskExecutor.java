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
import edu.iu.dsc.tws.common.worker.DriverListener;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.master.worker.JMWorkerMessenger;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class HTGTaskExecutor extends TaskExecutor implements DriverListener {
  private static final Logger LOG = Logger.getLogger(HTGTaskExecutor.class.getName());

  private BlockingQueue<HTGJobAPI.ExecuteMessage> executeMessageQueue;

  private boolean executionCompleted;

  public HTGTaskExecutor(Config cfg, int wId, List<JobMasterAPI.WorkerInfo> workerInfoList,
                         Communicator net) {
    super(cfg, wId, workerInfoList, net);
    this.executeMessageQueue = new LinkedBlockingQueue<>();
    this.executionCompleted = false;
  }

  public boolean execute(Map<String, DataFlowTaskGraph> dataFlowTaskGraphMap) {
    HTGJobAPI.ExecuteMessage msg;
    DataFlowTaskGraph taskGraph;
    ExecutionPlan executionPlan;
    HTGJobAPI.ExecuteCompletedMessage subGraphCompletedMsg;

    JMWorkerMessenger workerMessenger = JMWorkerAgent.getJMWorkerAgent().getJMWorkerMessenger();

    while (!this.executionCompleted) {
      try {
        msg = executeMessageQueue.take();

        String subGraph = msg.getSubgraphName();
        LOG.info("Executing the subgraph : " + subGraph);

        // get the subgraph from the map
        taskGraph = dataFlowTaskGraphMap.get(subGraph);
        // use the taskexecutor to create the execution plan
        executionPlan = this.plan(taskGraph);
        // reuse the task executor execute
        this.execute(taskGraph, executionPlan);

        subGraphCompletedMsg =
            HTGJobAPI.ExecuteCompletedMessage.newBuilder().setSubgraphName(subGraph).build();
        if (!workerMessenger.sendToDriver(subGraphCompletedMsg)) {
          LOG.severe("Unable to send the subgraph completed message :" + subGraphCompletedMsg);
        }
      } catch (InterruptedException e) {
        LOG.info("Unable to take the message from the queue");
      }
    }

    return true;
  }

  @Override
  public void workersScaledUp(int instancesAdded) {
    LOG.info("Workers scaled up msg received. Instances added: " + instancesAdded);
  }

  @Override
  public void workersScaledDown(int instancesRemoved) {
    LOG.info("Workers scaled down msg received. Instances removed: " + instancesRemoved);
  }

  @Override
  public void broadcastReceived(Any anyMessage) {
    LOG.info("Broadcast received from the Driver" + anyMessage);
    if (anyMessage.is(HTGJobAPI.ExecuteMessage.class)) {
      try {
        HTGJobAPI.ExecuteMessage executeMessage = anyMessage.unpack(HTGJobAPI.ExecuteMessage.class);
        LOG.info("Received Execute message. Execute message: " + executeMessage);

        this.executeMessageQueue.put(executeMessage);

      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Unable to unpack received protocol buffer message as broadcast", e);
      } catch (InterruptedException e) {
        LOG.log(Level.SEVERE, "Unable to insert message to the queue", e);
      }
    } else if (anyMessage.is(HTGJobAPI.HTGJobCompletedMessage.class)) {
//      HTGJobAPI.HTGJobCompletedMessage htgJobCompletedMessage = null;
//      try {
//        htgJobCompletedMessage = anyMessage.unpack(HTGJobAPI.HTGJobCompletedMessage.class);
//        this.executionCompleted = true;
//      } catch (InvalidProtocolBufferException e) {
//        LOG.log(Level.SEVERE, "Unable to unpack received protocol buffer message as broadcast",
//        e);
//      }
//     LOG.info(, "Received HTG job completed: " + htgJobCompletedMessage);
      this.executionCompleted = true;
      LOG.info("Received HTG job completed message");
    } else {
      LOG.warning("Unknown message for htg task execution");
    }
  }
}
