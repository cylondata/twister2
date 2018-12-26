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
package edu.iu.dsc.tws.api.htgjob;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.htg.HTGTaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public final class Twister2HTGSubmitter {
  private static final Logger LOG = Logger.getLogger(Twister2HTGSubmitter.class.getName());

  /**
   * Configuration
   */
  private Config config;

  /**
   * The serializer
   */
  private KryoMemorySerializer kryoMemorySerializer;

  /**
   * The driver for listening and sending messages to workers
   */
  private Twister2HTGDriver driver;

  /**
   * The queue to coordinate between driver and submitter
   */
  private BlockingQueue<DriverEvent> inDriverEvents = new LinkedBlockingDeque<>();

  /**
   * This submitter keeps track of state
   */
  private DriverState driverState = DriverState.INITIALIZE;

  /**
   * The driver messenger
   */
  private IDriverMessenger messenger;

  public Twister2HTGSubmitter(Config cfg) {
    this.config = cfg;
    this.kryoMemorySerializer = new KryoMemorySerializer();
    // set the driver events queue, this will make sure that we only create one instance of
    // submitter
    Twister2HTGInstance.getTwister2HTGInstance().setDriverEvents(inDriverEvents);
  }

  /**
   * The executeHTG method first call the schedule method to get the schedule list of the HTG.
   * Then, it invokes the build HTG Job object to build the htg job object for the scheduled graphs.
   */
  public void execute(SubGraphJob graph) {
    LOG.fine("Starting task graph Requirements:" + graph.getGraph().getTaskGraphName());

    if (!(driverState == DriverState.JOB_FINISHED || driverState == DriverState.INITIALIZE)) {
      // now we need to send messages
      throw new RuntimeException("Invalid state to execute a job: " + driverState);
    }

    HTGJobAPI.SubGraph job = buildHTGJob(graph);
    // this is the first time
    if (driverState == DriverState.INITIALIZE) {
      startWorkers(job);
      driverState = DriverState.WAIT_FOR_WORKERS_TO_START;
      // lets wait until the worker start message received
      try {
        waitForEvent(DriveEventType.INITIALIZE);
        driverState = DriverState.DRIVER_LISTENER_INITIALIZED;
        // now submit the job
        submitJob(job);
        driverState = DriverState.JOB_SUBMITTED;
        // lets wait for another event
        waitForEvent(DriveEventType.FINISHED_JOB);
        driverState = DriverState.JOB_FINISHED;
      } catch (Exception e) {
        throw new RuntimeException("Driver is not initialized", e);
      }
      // now lets submit the
    } else if (driverState == DriverState.JOB_FINISHED) {
      submitJob(job);
      driverState = DriverState.JOB_SUBMITTED;
      // lets wait for another event
      try {
        waitForEvent(DriveEventType.FINISHED_JOB);
        driverState = DriverState.JOB_FINISHED;
      } catch (Exception e) {
        throw new RuntimeException("Driver is not initialized", e);
      }
    }
  }

  /**
   * Send the job as a serialized protobuf to all the workers
   *
   * @param job subgraph
   */
  public void submitJob(HTGJobAPI.SubGraph job) {
    HTGJobAPI.ExecuteMessage.Builder builder = HTGJobAPI.ExecuteMessage.newBuilder();
    builder.setSubgraphName(job.getName());
    builder.setGraph(job);
    messenger.broadcastToAllWorkers(builder.build());
  }

  /**
   * This method is responsible for building the htg job object which is based on the outcome of
   * the scheduled graphs list.
   */
  private HTGJobAPI.SubGraph buildHTGJob(SubGraphJob job) {
    JobAPI.Config.Builder configBuilder = JobAPI.Config.newBuilder();

    JobConfig jobConfig = job.getJobConfig();
    DataFlowTaskGraph graph = job.getGraph();

    jobConfig.forEach((key, value) -> {
      byte[] objectByte = kryoMemorySerializer.serialize(value);
      configBuilder.putConfigByteMap(key, ByteString.copyFrom(objectByte));
    });

    byte[] graphBytes = kryoMemorySerializer.serialize(graph);

    //Construct the HTGJob object to be sent to the HTG Driver
    return HTGJobAPI.SubGraph.newBuilder()
        .setName(graph.getTaskGraphName())
        .setConfig(configBuilder)
        .setGraphSerialized(ByteString.copyFrom(graphBytes))
        .build();
  }

  private void startWorkers(HTGJobAPI.SubGraph htgJob) {
    //send the singleton object to the HTG Driver
    Twister2HTGInstance twister2HTGInstance = Twister2HTGInstance.getTwister2HTGInstance();
    twister2HTGInstance.setHtgSchedulerClassName(Twister2HTGScheduler.class.getName());

    //Setting the first graph resource requirements for the initial resource allocation
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName(htgJob.getName())
        .setWorkerClass(HTGTaskWorker.class.getName())
        .setDriverClass(Twister2HTGDriver.class.getName())
        .addComputeResource(htgJob.getCpu(), htgJob.getRamMegaBytes(),
            htgJob.getDiskGigaBytes(), htgJob.getInstances())
        .build();

    Twister2Submitter.submitJob(twister2Job, config);
  }

  private DriverEvent waitForEvent(DriveEventType type) throws Exception {
    // lets wait for driver events
    try {
      DriverEvent event = inDriverEvents.take();
      if (event.getType() != type) {
        throw new Exception("Un-expected event: " + type);
      }
      return event;
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to take event", e);
    }
  }
}
