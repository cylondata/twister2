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

import java.util.HashMap;
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
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.master.worker.JMWorkerMessenger;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class HTGTaskExecutor implements JobListener {
  private static final Logger LOG = Logger.getLogger(HTGTaskExecutor.class.getName());

  private BlockingQueue<Any> executeMessageQueue;

  private int workerId;

  private KryoMemorySerializer serializer;

  private Map<String, Map<String, DataSet<Object>>> outPuts = new HashMap<>();

  private TaskExecutor taskExecutor;

  public HTGTaskExecutor(Config cfg, int wId, List<JobMasterAPI.WorkerInfo> workerInfoList,
                         Communicator net) {
    taskExecutor = new TaskExecutor(cfg, wId, workerInfoList, net);
    this.executeMessageQueue = new LinkedBlockingQueue<>();
    this.workerId = wId;
    this.serializer = new KryoMemorySerializer();
  }

  /**
   * execute
   */
  public boolean execute() {
    Any msg;
    while (true) {
      try {
        msg = executeMessageQueue.take();

        if (msg.is(HTGJobAPI.ExecuteMessage.class)) {
          if (handleExecuteMessage(msg)) {
            return false;
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

  private boolean handleExecuteMessage(Any msg) {
    JMWorkerMessenger workerMessenger = JMWorkerAgent.getJMWorkerAgent().getJMWorkerMessenger();
    HTGJobAPI.ExecuteMessage executeMessage;
    String subgraph;
    ExecutionPlan executionPlan;
    HTGJobAPI.ExecuteCompletedMessage completedMessage;
    try {
      executeMessage = msg.unpack(HTGJobAPI.ExecuteMessage.class);
      LOG.log(Level.INFO, workerId + "Processing execute message: " + executeMessage);
      subgraph = executeMessage.getSubgraphName();
      LOG.log(Level.INFO, workerId + " Executing the subgraph : " + subgraph);

      // get the subgraph from the map
      HTGJobAPI.SubGraph subGraph = executeMessage.getGraph();
      DataFlowTaskGraph taskGraph = (DataFlowTaskGraph) serializer.deserialize(
          subGraph.getGraphSerialized().toByteArray());
      if (taskGraph == null) {
        LOG.severe(workerId + " Unable to find the subgraph " + subgraph);
        return true;
      }
      // use the taskexecutor to create the execution plan
      executionPlan = taskExecutor.plan(taskGraph);
      LOG.log(Level.INFO, workerId + " exec plan : " + executionPlan);
      LOG.log(Level.INFO, workerId + " exec plan : " + executionPlan.getNodes());

      List<HTGJobAPI.Input> inputs = subGraph.getInputsList();
      // now lets get those inputs
      for (HTGJobAPI.Input in : inputs) {
        DataSet<Object> dataSet = outPuts.get(in.getParentGraph()).get(in.getName());
        taskExecutor.addSourceInput(taskGraph, executionPlan, in.getName(), dataSet);
      }

      // reuse the task executor execute
      taskExecutor.execute(taskGraph, executionPlan);

      LOG.log(Level.INFO, workerId + " Completed subgraph : " + subgraph);

      LOG.log(Level.INFO, workerId + " Sending subgraph completed message to driver");
      completedMessage = HTGJobAPI.ExecuteCompletedMessage.newBuilder()
          .setSubgraphName(subgraph).build();

      List<String> outPutNames = subGraph.getOutputsList();
      Map<String, DataSet<Object>> outs = new HashMap<>();
      for (String out : outPutNames) {
        // get the outputs
        DataSet<Object> outPut = taskExecutor.getSinkOutput(taskGraph, executionPlan, out);
        outs.put(out, outPut);
      }
      outPuts.put("out", outs);

      if (!workerMessenger.sendToDriver(completedMessage)) {
        LOG.severe("Unable to send the subgraph completed message :" + completedMessage);
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Unable to unpack received message ", e);
    }
    return false;
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
