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
package edu.iu.dsc.tws.api.cdfw;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Any;

import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.htg.CDFWWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.driver.DriverJobListener;
import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.master.driver.DriverMessenger;
import edu.iu.dsc.tws.master.driver.JMDriverAgent;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceRuntime;

public final class CDFWExecutor implements DriverJobListener {
  private static final Logger LOG = Logger.getLogger(CDFWExecutor.class.getName());

  /**
   * Configuration
   */
  private Config config;

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

  /**
   * The submitter thread
   */
  private Thread submitterThread;

  /**
   * Keep track of how many jobs submitted through
   */
  private int jobCount = 0;

  /**
   * Driver agent
   */
  private JMDriverAgent driverAgent;

  public CDFWExecutor(Config cfg) {
    this.config = cfg;
    // set the driver events queue, this will make sure that we only create one instance of
    // submitter
    Twister2HTGInstance.getTwister2HTGInstance().setDriverEvents(inDriverEvents);
  }

  /**
   * The executeHTG method first call the schedule method to get the schedule list of the HTG.
   * Then, it invokes the build HTG Job object to build the htg job object for the scheduled graphs.
   */
  public void execute(DataFlowGraph graph) {
    LOG.info("Starting task graph Requirements:" + graph.getGraphName());

    if (!(driverState == DriverState.JOB_FINISHED || driverState == DriverState.INITIALIZE)) {
      // now we need to send messages
      throw new RuntimeException("Invalid state to execute a job: " + driverState);
    }
    jobCount++;

    HTGJobAPI.SubGraph job = buildHTGJob(graph);
    // this is the first time
    if (driverState == DriverState.INITIALIZE) {
      submitterThread = new Thread(new SubmitterRunnable(job));
      submitterThread.start();
      try {
        Thread.sleep(2000);
      } catch (InterruptedException ignore) {
      }
      // set the workers as number of instances
      startDriver(job.getInstances());

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

  public void close() {
    // send the close message
    sendCloseMessage();
    // lets wait for the submitter thread to finish
    try {
      driverAgent.close();
      LOG.log(Level.INFO, "Waiting for submitter thread");
      submitterThread.join();
      LOG.log(Level.INFO, "Submitter thread finished, we are closed");
    } catch (InterruptedException ignore) {
    }
  }

  private void sendCloseMessage() {
    HTGJobAPI.HTGJobCompletedMessage.Builder builder = HTGJobAPI.HTGJobCompletedMessage.
        newBuilder().setHtgJobname("");
    messenger.broadcastToAllWorkers(builder.build());
  }

  /**
   * Send the job as a serialized protobuf to all the workers
   *
   * @param job subgraph
   */
  private void submitJob(HTGJobAPI.SubGraph job) {
    LOG.log(Level.INFO, "Sending graph to workers for execution: " + job.getName());
    HTGJobAPI.ExecuteMessage.Builder builder = HTGJobAPI.ExecuteMessage.newBuilder();
    builder.setSubgraphName(job.getName());
    builder.setGraph(job);
    messenger.broadcastToAllWorkers(builder.build());
  }

  /**
   * This method is responsible for building the htg job object which is based on the outcome of
   * the scheduled graphs list.
   */
  private HTGJobAPI.SubGraph buildHTGJob(DataFlowGraph job) {
    return job.build();
  }

  @Override
  public void workerMessageReceived(Any anyMessage, int senderWorkerID) {
    LOG.log(Level.INFO, String.format("Received worker message %d: %s", senderWorkerID,
        anyMessage.getClass().getName()));
    inDriverEvents.offer(new DriverEvent(DriveEventType.FINISHED_JOB, anyMessage));
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
    inDriverEvents.offer(new DriverEvent(DriveEventType.INITIALIZE, null));
  }

  private class SubmitterRunnable implements Runnable {
    private HTGJobAPI.SubGraph htgJob;

    SubmitterRunnable(HTGJobAPI.SubGraph job) {
      this.htgJob = job;
    }

    @Override
    public void run() {
      startWorkers(htgJob);
    }
  }

  /**
   * Start the workers by submitting the first job to the workers. This will start the workers,
   * but task graph will not run until the job is submmitted
   *
   * @param htgJob subgraph
   */
  private void startWorkers(HTGJobAPI.SubGraph htgJob) {
    //send the singleton object to the HTG Driver
    Twister2HTGInstance twister2HTGInstance = Twister2HTGInstance.getTwister2HTGInstance();
    twister2HTGInstance.setHtgSchedulerClassName(DefaultScheduler.class.getName());

    //Setting the first graph resource requirements for the initial resource allocation
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName(htgJob.getName())
        .setWorkerClass(CDFWWorker.class.getName())
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

  private void startDriver(int numberOfWorkers) {
    long start = System.currentTimeMillis();
    while (ResourceRuntime.getInstance().getJobMasterHost() == null) {
      if ((System.currentTimeMillis() - start) > 1000) {
        return;
      }
    }
    // first start JMDriverAgent
    String jobMasterIP = ResourceRuntime.getInstance().getJobMasterHost();
    int jmPort = ResourceRuntime.getInstance().getJobMasterPort();

    driverAgent =
        JMDriverAgent.createJMDriverAgent(config, jobMasterIP, jmPort, numberOfWorkers);
    driverAgent.startThreaded();
    // construct DriverMessenger
    messenger = new DriverMessenger(driverAgent);

    // add listener to receive worker messages
    JMDriverAgent.addDriverJobListener(this);
  }

  private String getJobName(String name) {
    if (name == null) {
      return "graph_" + jobCount;
    }
    return name;
  }
}
