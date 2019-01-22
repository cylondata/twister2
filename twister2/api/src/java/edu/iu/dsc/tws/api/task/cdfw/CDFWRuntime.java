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
package edu.iu.dsc.tws.api.task.cdfw;

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
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.master.worker.JMWorkerMessenger;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.CDFWJobAPI;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class CDFWRuntime implements JobListener {
  private static final Logger LOG = Logger.getLogger(CDFWRuntime.class.getName());

  /**
   * Messages from the driver
   */
  private BlockingQueue<Any> executeMessageQueue;

  /**
   * Worker id
   */
  private int workerId;

  /**
   * Kryo serializer
   */
  private KryoMemorySerializer serializer;

  /**
   * The outputs from previous graphs
   * [graph, [output name, data set]]
   */
  private Map<String, Map<String, DataObject<Object>>> outPuts = new HashMap<>();

  /**
   * Task executor
   */
  private TaskExecutor taskExecutor;

  /**
   * Connected dataflow runtime
   *
   * @param cfg configuration
   * @param wId worker id
   * @param workerInfoList worker information list
   * @param net network
   */
  public CDFWRuntime(Config cfg, int wId, List<JobMasterAPI.WorkerInfo> workerInfoList,
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

        if (msg.is(CDFWJobAPI.ExecuteMessage.class)) {
          if (handleExecuteMessage(msg)) {
            return false;
          }
        } else if (msg.is(CDFWJobAPI.CDFWJobCompletedMessage.class)) {
          LOG.log(Level.INFO, workerId + "Received CDFW job completed message. Leaving execution "
              + "loop");
          break;
        } else {
          LOG.log(Level.WARNING, workerId + "Unknown message for cdfw task execution");
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
    CDFWJobAPI.ExecuteMessage executeMessage;
    ExecutionPlan executionPlan;
    CDFWJobAPI.ExecuteCompletedMessage completedMessage;
    try {
      executeMessage = msg.unpack(CDFWJobAPI.ExecuteMessage.class);
//      LOG.log(Level.INFO, workerId + "Processing execute message: " + executeMessage);
//      LOG.log(Level.INFO, workerId + " Executing the subgraph : " + subgraph);

      // get the subgraph from the map
      CDFWJobAPI.SubGraph subGraph = executeMessage.getGraph();
      DataFlowTaskGraph taskGraph = (DataFlowTaskGraph) serializer.deserialize(
          subGraph.getGraphSerialized().toByteArray());
      if (taskGraph == null) {
        LOG.severe(workerId + " Unable to find the subgraph " + subGraph.getName());
        return true;
      }
      // use the taskexecutor to create the execution plan
      executionPlan = taskExecutor.plan(taskGraph);
//      LOG.log(Level.INFO, workerId + " exec plan : " + executionPlan);
//      LOG.log(Level.INFO, workerId + " exec plan : " + executionPlan.getNodes());

      if (subGraph.getInputsList().size() != 0) {
        for (CDFWJobAPI.Input input : subGraph.getInputsList()) {
          String inputName = input.getName();
          String inputGraph = input.getParentGraph();

          if (!outPuts.containsKey(inputGraph)) {
            throw new RuntimeException("We cannot find the input graph: " + inputGraph);
          }

          Map<String, DataObject<Object>> outsPerGraph = outPuts.get(inputGraph);

          if (!outsPerGraph.containsKey(inputName)) {
            throw new RuntimeException("We cannot find the input: " + inputName);
          }

          DataObject<Object> outPutObject = outsPerGraph.get(inputName);
          taskExecutor.addSourceInput(taskGraph, executionPlan, inputName, outPutObject);
        }
      }

      List<CDFWJobAPI.Input> inputs = subGraph.getInputsList();
      // now lets get those inputs
      for (CDFWJobAPI.Input in : inputs) {
        DataObject<Object> dataSet = outPuts.get(in.getParentGraph()).get(in.getName());
        taskExecutor.addSourceInput(taskGraph, executionPlan, in.getName(), dataSet);
      }

      // reuse the task executor execute
      taskExecutor.execute(taskGraph, executionPlan);

      LOG.log(Level.INFO, workerId + " Completed subgraph : " + subGraph.getName());

//      LOG.log(Level.INFO, workerId + " Sending subgraph completed message to driver");
      completedMessage = CDFWJobAPI.ExecuteCompletedMessage.newBuilder()
          .setSubgraphName(subGraph.getName()).build();

      List<String> outPutNames = subGraph.getOutputsList();
      Map<String, DataObject<Object>> outs = new HashMap<>();
      for (String out : outPutNames) {
        // get the outputs
        DataObject<Object> outPut = taskExecutor.getSinkOutput(taskGraph, executionPlan, out);
        outs.put(out, outPut);
      }
      outPuts.put(subGraph.getName(), outs);

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
  public void driverMessageReceived(Any anyMessage) {
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
