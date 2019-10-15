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
package edu.iu.dsc.tws.task.impl.cdfw;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.Vertex;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.JobListener;
import edu.iu.dsc.tws.api.resource.Network;
import edu.iu.dsc.tws.api.util.KryoSerializer;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.master.worker.JMWorkerMessenger;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.CDFWJobAPI;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

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
  private KryoSerializer serializer;

  /**
   * The outputs from previous graphs
   * [graph, [output name, data set]]
   */
  private Map<String, Map<String, DataObject<Object>>> outPuts = new HashMap<>();


  private Map<String, DataObject<Object>> iterativeOutPuts = new HashMap<>();

  /**
   * Task executor
   */
  private TaskExecutor taskExecutor;

  private Config config;

  private IWorkerController controller;

  private Communicator communicator;

  private TWSChannel channel;


  /**
   * Connected Dataflow Runtime
   */
  public CDFWRuntime(Config cfg, int wId, IWorkerController controller) {
    this.executeMessageQueue = new LinkedBlockingQueue<>();
    this.workerId = wId;
    this.serializer = new KryoSerializer();
    this.controller = controller;
    this.config = cfg;

    List<JobMasterAPI.WorkerInfo> workerInfoList = initSynch(controller);

    /*try {
      workerInfoList = controller.getAllWorkers();
      LOG.info("worker info list @ cons:" + workerInfoList);
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }*/

    if (workerInfoList == null) {
      return;
    }

    // create the channel
    channel = Network.initializeChannel(config, controller);
    String persistent = null;

    // create the communicator
    communicator = new Communicator(config, channel, persistent);
    taskExecutor = new TaskExecutor(cfg, wId, workerInfoList, communicator, null);
  }


  private List<JobMasterAPI.WorkerInfo> initSynch(IWorkerController workerController) {
    // wait all workers to join the job
    List<JobMasterAPI.WorkerInfo> workerList = null;
    try {
      workerList = workerController.getAllWorkers();
      LOG.info("worker info list @ cons:" + workerList);
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return null;
    }
    if (workerList == null) {
      LOG.severe("Can not get all workers to join. Something wrong. Exiting ....................");
      return null;
    }

    LOG.info(workerList.size() + " workers joined. ");

    // syncs with all workers
    LOG.info("Waiting on a barrier ........................ ");
    try {
      workerController.waitOnBarrier();
    } catch (TimeoutException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }

    LOG.info("Proceeded through the barrier ........................ ");
    return workerList;
  }

  /**
   * execute
   */
  public boolean execute() {
    Any msg;
    while (true) {
      msg = executeMessageQueue.peek();
      if (msg == null) {
        if (scaleUpRequest.get()) {

//          List<JobMasterAPI.WorkerInfo> workerInfoList = null;
//          try {
//            workerInfoList = controller.getAllWorkers();
//            LOG.info("worker info list @ execute:" + workerInfoList);
//          } catch (TimeoutException timeoutException) {
//            LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
//          }

          communicator.close();
          List<JobMasterAPI.WorkerInfo> workerInfoList = initSynch(controller);
          LOG.info("worker info list @ execute:" + workerInfoList);


          // create the channel
          LOG.info("Existing workers calling barrier");
          channel = Network.initializeChannel(config, controller);
          String persistent = null;

          // create the communicator
          communicator = new Communicator(config, channel, persistent);
          taskExecutor = new TaskExecutor(config, workerId, workerInfoList, communicator, null);
        }
        scaleUpRequest.set(false);
        continue;
      }
      msg = executeMessageQueue.poll();
      if (msg.is(CDFWJobAPI.ExecuteMessage.class)) {
        if (handleExecuteMessage(msg)) {
          return false;
        }
      } else if (msg.is(CDFWJobAPI.CDFWJobCompletedMessage.class)) {
        LOG.log(Level.INFO, workerId + "Received CDFW job completed message. Leaving execution "
            + "loop");
        break;
      }
    }
    LOG.log(Level.INFO, workerId + " Execution Completed");
    return true;
  }

  private boolean reinitialize() {
    communicator.close();
    List<JobMasterAPI.WorkerInfo> workerInfoList = null;
    try {
      workerInfoList = controller.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
    }

    //LOG.info("Reinitialize Method Getting Called:" + workerInfoList);
    // create the channel
    channel = Network.initializeChannel(config, controller);
    String persistent = null;

    // create the communicator
    communicator = new Communicator(config, channel, persistent);
    taskExecutor = new TaskExecutor(config, workerId, workerInfoList, communicator, null);
    return true;
  }

  private boolean handleExecuteMessage(Any msg) {
    JMWorkerMessenger workerMessenger = JMWorkerAgent.getJMWorkerAgent().getJMWorkerMessenger();
    CDFWJobAPI.ExecuteMessage executeMessage;
    ExecutionPlan executionPlan;
    CDFWJobAPI.ExecuteCompletedMessage completedMessage = null;
    try {
      executeMessage = msg.unpack(CDFWJobAPI.ExecuteMessage.class);
      // get the subgraph from the map
      CDFWJobAPI.SubGraph subGraph = executeMessage.getGraph();
      ComputeGraph taskGraph = (ComputeGraph) serializer.deserialize(
          subGraph.getGraphSerialized().toByteArray());
      if (taskGraph == null) {
        LOG.severe(workerId + " Unable to find the subgraph " + subGraph.getName());
        return true;
      }

      // use the taskexecutor to create the execution plan
      executionPlan = taskExecutor.plan(taskGraph);

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

      if (subGraph.getGraphType().equals(Context.GRAPH_TYPE)) {
        List<CDFWJobAPI.Input> inputs1 = subGraph.getInputsList();
        for (CDFWJobAPI.Input in : inputs1) {
          for (String key : iterativeOutPuts.keySet()) {
            taskExecutor.addSourceInput(taskGraph, executionPlan, key, iterativeOutPuts.get(key));
            if (!in.getName().equals(key)) {
              DataObject<Object> dataSet = outPuts.get(in.getParentGraph()).get(in.getName());
              taskExecutor.addSourceInput(taskGraph, executionPlan, in.getName(), dataSet);
            }
          }
        }
      }

      if (subGraph.getGraphType().equals(Context.GRAPH_TYPE)) {
        taskExecutor.itrExecute(taskGraph, executionPlan);
      } else {
        taskExecutor.execute(taskGraph, executionPlan);
      }
     
      //reuse the task executor execute
      completedMessage = CDFWJobAPI.ExecuteCompletedMessage.newBuilder()
          .setSubgraphName(subGraph.getName()).build();

      Map<String, DataObject<Object>> outs = new HashMap<>();
      if (subGraph.getOutputsList().size() != 0) {
        List<CDFWJobAPI.Output> outPutNames = subGraph.getOutputsList();
        for (CDFWJobAPI.Output outputs : outPutNames) {
          DataObject<Object> outPut = taskExecutor.getOutput(
              taskGraph, executionPlan, outputs.getTaskname());
          outs.put(outputs.getName(), outPut);
        }
        outPuts.put(subGraph.getName(), outs);
      }

      if (subGraph.getGraphType().equals(Context.GRAPH_TYPE)) {
        processIterativeOuput(taskGraph, executionPlan);
      }

      if (!workerMessenger.sendToDriver(completedMessage)) {
        LOG.severe("Unable to send the subgraph completed message :" + completedMessage);
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Unable to unpack received message ", e);
    }
    return false;
  }

  private void processIterativeOuput(ComputeGraph taskGraph, ExecutionPlan executionPlan) {
    DataObject<Object> outPut;
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(taskGraph.getTaskVertexSet());
    for (Vertex vertex : taskVertexSet) {
      INode iNode = vertex.getTask();
      if (iNode instanceof Collector) {
        Set<String> collectibleNameSet = ((Collector) iNode).getCollectibleNames();
        outPut = taskExecutor.getOutput(taskGraph, executionPlan, vertex.getName());
        iterativeOutPuts.put(collectibleNameSet.toString(), outPut);
      }
    }
  }

  private AtomicBoolean scaleUpRequest = new AtomicBoolean(false);

  @Override
  public void workersScaledUp(int instancesAdded) {
    LOG.log(Level.INFO, workerId + "Workers scaled up msg received. Instances added: "
        + instancesAdded);
    scaleUpRequest.set(true);
  }

  @Override
  public void workersScaledDown(int instancesRemoved) {
    LOG.log(Level.INFO, workerId + "Workers scaled down msg received. Instances removed: "
        + instancesRemoved);
    communicator.close();

    List<JobMasterAPI.WorkerInfo> workerInfoList;
    try {
      workerInfoList = controller.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    // create the channel
    channel = Network.initializeChannel(config, controller);
    String persistent = null;

    // create the communicator
    communicator = new Communicator(config, channel, persistent);
    taskExecutor = new TaskExecutor(config, workerId, workerInfoList, communicator, null);
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
