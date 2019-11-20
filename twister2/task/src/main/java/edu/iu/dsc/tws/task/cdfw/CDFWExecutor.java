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
package edu.iu.dsc.tws.task.cdfw;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Any;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.proto.system.job.CDFWJobAPI;

public final class CDFWExecutor {
  private static final Logger LOG = Logger.getLogger(CDFWExecutor.class.getName());

  /**
   * The queue to coordinate between driver and submitter
   */
  private BlockingQueue<DriverEvent> driverEvents = new LinkedBlockingDeque<>();

  /**
   * This submitter keeps track of state
   */
  private DriverState driverState = DriverState.INITIALIZE;

  /**
   * The driver messenger
   */
  private IDriverMessenger driverMessenger;

  /**
   * Execution env object to get the information about the workers
   */
  private CDFWEnv executionEnv;

  public CDFWExecutor(CDFWEnv executionEnv, IDriverMessenger messenger) {
    this.driverMessenger = messenger;
    this.executionEnv = executionEnv;
  }

  /**submitJob
   * The executeCDFW method first call the schedule method to get the schedule list of the CDFW.
   * Then, it invokes the build CDFW Job object to build the cdfw job object for the scheduled graphs.
   */
  public void execute(DataFlowGraph graph) {
    LOG.fine("Starting task graph Requirements:" + graph.getGraphName());
    if (!(driverState == DriverState.JOB_FINISHED || driverState == DriverState.INITIALIZE)) {
      // now we need to send messages
      throw new RuntimeException("Invalid state to execute a job: " + driverState);
    }
    //LOG.info("Worker List Size(first exec):" + this.executionEnv.getWorkerInfoList().size());
    CDFWScheduler cdfwScheduler = new CDFWScheduler(this.executionEnv.getWorkerInfoList());
    Set<Integer> workerIDs = cdfwScheduler.schedule(graph);
    submitGraph(graph, workerIDs);
  }

  /**
   * The executeCDFW method first call the schedule method to get the schedule list of the CDFW.
   * Then, it invokes the buildCDFWJob method to build the job object for the scheduled graphs.
   */
  public void executeCDFW(DataFlowGraph... graph) {

    if (!(driverState == DriverState.JOB_FINISHED || driverState == DriverState.INITIALIZE)) {
      // now we need to send messages
      throw new RuntimeException("Invalid state to execute a job: " + driverState);
    }
    CDFWScheduler cdfwScheduler = new CDFWScheduler(this.executionEnv.getWorkerInfoList());
    Map<DataFlowGraph, Set<Integer>> scheduleGraphMap = cdfwScheduler.schedule(graph);
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(scheduleGraphMap.size());
    for (Map.Entry<DataFlowGraph, Set<Integer>> entry : scheduleGraphMap.entrySet()) {
      CDFWExecutorTask cdfwSchedulerTask = new CDFWExecutorTask(entry.getKey(), entry.getValue());
      executor.submit(cdfwSchedulerTask);
    }
    try {
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new Twister2RuntimeException(e);
    } finally {
      executor.shutdown();
    }
  }

  void close() {
    //send the close message
    sendCloseMessage();
  }

  private void submitGraph(DataFlowGraph dataFlowgraph, Set<Integer> workerIDs) {
    if (driverState == DriverState.INITIALIZE || driverState == DriverState.JOB_FINISHED) {
      try {
        //build the schedule plan for the dataflow graph
        DataFlowGraph dataFlowGraph = buildCDFWSchedulePlan(dataFlowgraph, workerIDs);
        CDFWJobAPI.SubGraph job = buildCDFWJob(dataFlowGraph);
        //now submit the job
        submitJob(job);
        driverState = DriverState.JOB_SUBMITTED;
        // lets wait for another event
        waitForEvent(DriveEventType.FINISHED_JOB);
        driverState = DriverState.JOB_FINISHED;
      } catch (Exception e) {
        throw new Twister2RuntimeException("Driver is not initialized", e);
      }
    } else {
      throw new Twister2RuntimeException("Failed to submit job in this state: " + driverState);
    }
  }

  private DataFlowGraph buildCDFWSchedulePlan(DataFlowGraph dataFlowGraph,
                                              Set<Integer> workerIDs) {
    dataFlowGraph.setCdfwSchedulePlans(
        CDFWJobAPI.CDFWSchedulePlan.newBuilder().addAllWorkers(workerIDs).build());
    return dataFlowGraph;
  }

  private void sendCloseMessage() {
    CDFWJobAPI.CDFWJobCompletedMessage.Builder builder = CDFWJobAPI.CDFWJobCompletedMessage.
        newBuilder().setHtgJobname("");
    driverMessenger.broadcastToAllWorkers(builder.build());
  }

  /**
   * Send the job as a serialized protobuf to all the workers
   *
   * @param job subgraph
   */
  private void submitJob(CDFWJobAPI.SubGraph job) {
    LOG.log(Level.INFO, "Sending graph to workers for execution: " + job.getName());
    CDFWJobAPI.ExecuteMessage.Builder builder = CDFWJobAPI.ExecuteMessage.newBuilder();
    builder.setSubgraphName(job.getName());
    builder.setGraph(job);
    driverMessenger.broadcastToAllWorkers(builder.build());
  }

  /**
   * This method is responsible for building the cdfw job object which is based on the outcome of
   * the scheduled graphs list.
   */
  private CDFWJobAPI.SubGraph buildCDFWJob(DataFlowGraph job) {
    return job.build();
  }

  void workerMessageReceived(Any anyMessage, int senderWorkerID) {
    LOG.log(Level.FINE, String.format("Received worker message %d: %s", senderWorkerID,
        anyMessage.getClass().getName()));
    driverEvents.offer(new DriverEvent(DriveEventType.FINISHED_JOB, anyMessage, senderWorkerID));
  }

  private DriverEvent waitForEvent(DriveEventType type) throws Exception {
    // lets wait for driver events
    try {
      DriverEvent event = driverEvents.take();
      if (event.getType() != type) {
        throw new Exception("Un-expected event: " + type);
      }
      return event;
    } catch (InterruptedException e) {
      throw new Twister2RuntimeException("Failed to take event", e);
    }
  }

  private class CDFWExecutorTask implements Runnable {

    private DataFlowGraph dataFlowGraph;
    private Set<Integer> workerIDs;

    CDFWExecutorTask(DataFlowGraph graph, Set<Integer> workerList) {
      this.dataFlowGraph = graph;
      this.workerIDs = workerList;
    }

    @Override
    public void run() {
      submitGraph(dataFlowGraph, workerIDs);
    }
  }
}
