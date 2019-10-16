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
package edu.iu.dsc.tws.master.server;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.IWorkerStatusListener;
import edu.iu.dsc.tws.common.driver.IDriver;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.master.dashclient.DashboardClient;
import edu.iu.dsc.tws.master.dashclient.models.JobState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * This class monitors upto date list of workers in a job with their status
 * It handles worker state changes:
 * worker join,
 * worker running,
 * worker completion,
 * worker failure
 * <p>
 * It gets worker state changes either from workers directly with protocol messages
 * or from ZooKeeper server
 * <p>
 * It handles Job Master to Dashboard communications
 * It handles Job Master to Driver interactions
 */
public class WorkerMonitor
    implements MessageHandler, IWorkerStatusListener, IWorkerFailureListener {

  private static final Logger LOG = Logger.getLogger(WorkerMonitor.class.getName());

  private JobMaster jobMaster;
  private RRServer rrServer;
  private DashboardClient dashClient;
  private IDriver driver;
  // whether this is a fault tolerant job
  private boolean faultTolerant;

  /**
   * numberOfWorkers in the job is tracked by this variable
   * all other classes in job master should get the upto-date numberOfWorkers from this variable
   * if is updated in the case of scale up and down
   */
  private int numberOfWorkers;

  private TreeMap<Integer, WorkerWithState> workers;

  public WorkerMonitor(JobMaster jobMaster,
                       RRServer rrServer,
                       DashboardClient dashClient,
                       JobAPI.Job job,
                       IDriver driver,
                       boolean faultTolerant) {

    this.jobMaster = jobMaster;
    this.rrServer = rrServer;
    this.dashClient = dashClient;
    this.driver = driver;
    this.numberOfWorkers = job.getNumberOfWorkers();
    this.faultTolerant = faultTolerant;

    workers = new TreeMap<>();
  }

  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  /**
   * get the size of workers list
   */
  public int getWorkersListSize() {
    return workers.size();
  }

  /**
   * get the list of currently registered workers
   */
  public Collection<WorkerWithState> getWorkerList() {
    return workers.values();
  }

  /**
   * get the list of workerIDs
   * @return
   */
  public List<Integer> getWorkerIDs() {
    return workers
        .values()
        .stream()
        .map(wws -> wws.getWorkerID())
        .collect(Collectors.toList());
  }

  /**
   * return true if there is a worker with the given workerID
   */
  public boolean existWorker(int workerID) {
    return workers.containsKey(workerID);
  }

  /**
   * get WorkerWithState for the given workerID
   */
  public WorkerWithState getWorkerWithState(int workerID) {
    return workers.get(workerID);
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {

    if (message instanceof JobMasterAPI.WorkerMessage) {
      LOG.log(Level.FINE, "WorkerMessage received: " + message.toString());
      JobMasterAPI.WorkerMessage workerMessage = (JobMasterAPI.WorkerMessage) message;
      workerMessageReceived(id, workerMessage);

    } else {
      LOG.log(Level.SEVERE, "Un-known message received: " + message);
    }
  }

  /**
   * new worker joins for the first
   */
  public void joined(JobMasterAPI.WorkerInfo workerInfo) {

    // if it is a regular join event
    // add the worker to worker list
    WorkerWithState worker = new WorkerWithState(workerInfo);
    worker.addWorkerState(JobMasterAPI.WorkerState.STARTING);
    workers.put(worker.getWorkerID(), worker);
    LOG.info("Worker: " + workerInfo.getWorkerID() + " joined the job.");

    // send worker registration message to dashboard
    if (dashClient != null) {
      dashClient.registerWorker(workerInfo, JobMasterAPI.WorkerState.STARTING);
    }
  }

  /**
   * if the worker is coming from failure
   */
  public void rejoined(JobMasterAPI.WorkerInfo workerInfo) {

    // update workerInfo and its status in the worker list
    WorkerWithState worker = new WorkerWithState(workerInfo);
    worker.addWorkerState(JobMasterAPI.WorkerState.RESTARTING);
    workers.put(worker.getWorkerID(), worker);
    LOG.info("WorkerID: " + workerInfo.getWorkerID() + " joined from failure.");

    // send worker registration message to dashboard
    if (dashClient != null) {
      dashClient.registerWorker(workerInfo, JobMasterAPI.WorkerState.RESTARTING);
    }

    // TODO inform checkpoint master
  }

  /**
   * called when a worker becomes running
   */
  public void running(int workerID) {

    workers.get(workerID).addWorkerState(JobMasterAPI.WorkerState.RUNNING);
    LOG.info("Worker: " + workerID + " became RUNNING.");

    // send worker state change message to dashboard
    if (dashClient != null) {
      dashClient.workerStateChange(workerID, JobMasterAPI.WorkerState.RUNNING);
    }

    // if all workers have become RUNNING, send job STARTED message
    if (haveAllWorkersBecomeRunning()) {
      jobMaster.allWorkersBecameRunning();
    }
  }

  /**
   * called when a worker COMPLETED the job
   */
  public void completed(int workerID) {
    workers.get(workerID).addWorkerState(JobMasterAPI.WorkerState.COMPLETED);
    LOG.info("Worker:" + workerID + " COMPLETED.");

    // send worker state change message to dashboard
    if (dashClient != null) {
      dashClient.workerStateChange(workerID, JobMasterAPI.WorkerState.COMPLETED);
    }

    // check whether all workers completed
    // if so, stop the job master
    // if all workers have completed, no need to send the response message back to the client
    if (allWorkersCompleted()) {
      LOG.info("All " + numberOfWorkers + " workers COMPLETED. Terminating the job.");
      jobMaster.completeJob(JobState.COMPLETED);
    }
  }

  /**
   * called when a worker FAILED
   */
  public void failed(int workerID) {
    workers.get(workerID).addWorkerState(JobMasterAPI.WorkerState.FAILED);
    LOG.info("Worker: " + workerID + " FAILED.");

    // send worker state change message to dashboard
    if (dashClient != null) {
      dashClient.workerStateChange(workerID, JobMasterAPI.WorkerState.FAILED);
    }

    // TODO: test whether this works
    // if this is a non-fault tolerant job, job needs to be terminated
    if (!faultTolerant) {
      LOG.info("A worker failed in a NON-FAULT TOLERANT job. Terminating the job.");
      jobMaster.completeJob(JobState.FAILED);
    }
  }

  public void workersScaledDown(int instancesRemoved) {

    // modify numberOfWorkers
    numberOfWorkers -= instancesRemoved;

    // construct scaled message to send to workers
    JobMasterAPI.WorkersScaled scaledMessage = JobMasterAPI.WorkersScaled.newBuilder()
        .setChange(0 - instancesRemoved)
        .setNumberOfWorkers(numberOfWorkers)
        .build();

    // construct killedWorkers list and remove those workers from workers list
    List<Integer> killedWorkers = new LinkedList<>();

    String strToLog = "Deleted worker IDs by scaling down: ";
    for (int i = 0; i < instancesRemoved; i++) {
      int killedID = numberOfWorkers + i;
      killedWorkers.add(killedID);
      workers.remove(killedID);
      rrServer.removeWorkerChannel(killedID);
      strToLog += killedID + ", ";
    }

    LOG.info(strToLog);

    // let all remaining workers know about the scaled message
    for (int workerID : workers.keySet()) {
      rrServer.sendMessage(scaledMessage, workerID);
    }

    // send Scale message to the dashboard
    if (dashClient != null) {
      dashClient.scaledWorkers(
          scaledMessage.getChange(), scaledMessage.getNumberOfWorkers(), killedWorkers);
    }
  }

  public void workersScaledUp(int instancesAdded) {

    // keep previous numberOfWorkers and update numberOfWorkers with new value
    int numberOfWorkersBeforeScaling = numberOfWorkers;
    numberOfWorkers += instancesAdded;

    JobMasterAPI.WorkersScaled scaledMessage = JobMasterAPI.WorkersScaled.newBuilder()
        .setChange(instancesAdded)
        .setNumberOfWorkers(numberOfWorkers)
        .build();

    // let all previous workers know about the scaled message
    // no need for informing newly added workers
    for (int wID = 0; wID < numberOfWorkersBeforeScaling; wID++) {
      rrServer.sendMessage(scaledMessage, wID);
    }

    // if all newly scaled up workers are already joined
    // send WorkersJoined messages
    if (allWorkersRegistered()) {
      sendWorkersJoinedMessage();
    }

    // send Scaled message to the dashboard
    if (dashClient != null) {
      dashClient.scaledWorkers(
          scaledMessage.getChange(), scaledMessage.getNumberOfWorkers(), new LinkedList<Integer>());
    }

  }

  public boolean broadcastMessage(Message message) {

    JobMasterAPI.DriverMessage driverMessage =
        JobMasterAPI.DriverMessage.newBuilder()
            .setData(Any.pack(message).toByteString())
            .build();

    // if all workers are not registered,
    // send a failure response message to the driver
    // do not send the broadcast message to any workers
    if (!allWorkersRegistered()) {
      LOG.warning("Could not send the broadcast message to all workers, "
          + "since they are not all registered.");
      return false;
    }

    // deliver the broadcast message to all workers
    for (int workerID : workers.keySet()) {
      boolean queued = rrServer.sendMessage(driverMessage, workerID);

      // if the message can not be queued, send a failure response
      // this may deliver the broadcast message to some workers but not to all
      // workers may be in an inconsistent state
      // TODO: we may need to find a solution to this
      if (!queued) {
        LOG.warning("Broadcast message can not be sent to workerID: " + workerID);
        return false;
      }
    }

    // TODO: before successfully completing,
    // we need to watch sendComplete events for the broadcast messages to the workers
    // that will make sure that the broadcast message is delivered to all workers

    return true;
  }

  /**
   * send a protocol buffer message to a list of workers
   */
  public boolean sendMessageToWorkerList(Message message, List<Integer> workerList) {

    JobMasterAPI.DriverMessage driverMessage =
        JobMasterAPI.DriverMessage.newBuilder()
            .setData(Any.pack(message).toByteString())
            .build();

    // if all workers are not registered,
    // send a failure response message to the driver
    // do not send the broadcast message to any workers
    for (int workerID : workerList) {
      WorkerWithState worker = workers.get(workerID);
      if (worker == null) {
        LOG.warning("There is no worker in JobMaster with workerID: " + workerID);
        return false;
      } else if (worker.getLastState() != JobMasterAPI.WorkerState.RUNNING
          && worker.getLastState() != JobMasterAPI.WorkerState.STARTING) {
        LOG.warning("workerID[" + workerID + "] is neither in RUNNING nor in STARTING scate. "
            + "Worker state: " + worker.getLastState());
        return false;
      }
    }

    // deliver the broadcast message to all workers
    for (int workerID : workerList) {
      boolean queued = rrServer.sendMessage(driverMessage, workerID);

      // if the message can not be queued, send a failure response
      // this may deliver the broadcast message to some workers but not to all
      // workers may be in an inconsistent state
      // TODO: we may need to find a solution to this
      if (!queued) {
        LOG.warning("Message can not be sent to workerID: " + workerID
            + " It is not sending the message to remaining workers in the list.");
        return false;
      }
    }

    // TODO: before successfully completing,
    // we need to watch sendComplete events for the broadcast messages to the workers
    // that will make sure that the broadcast message is delivered to all workers

    return true;
  }

  private void workerMessageReceived(RequestID id,
                                     JobMasterAPI.WorkerMessage workerMessage) {

    // first send the received message to the driver
    if (driver != null) {
      try {
        Any any = Any.parseFrom(workerMessage.getData());
        driver.workerMessageReceived(any, workerMessage.getWorkerID());
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Can not parse received protocol buffer message to Any", e);
        JobMasterAPI.WorkerMessageResponse failResponse =
            JobMasterAPI.WorkerMessageResponse.newBuilder()
                .setSucceeded(false)
                .setReason("Can not parse received protocol buffer message to Any")
                .build();
        rrServer.sendResponse(id, failResponse);
        LOG.warning("WorkerMessageResponse sent to the driver: \n" + failResponse);
        return;
      }
    }

    JobMasterAPI.WorkerMessageResponse successResponse =
        JobMasterAPI.WorkerMessageResponse.newBuilder()
            .setSucceeded(true)
            .build();

    rrServer.sendResponse(id, successResponse);
    LOG.fine("WorkerMessageResponse sent to the driver: \n" + successResponse);
  }

  /**
   * make sure that
   * all workers registered and their state may be anything
   * TODO: check the status of all workers. They have to be either STARTING or RUNNING.
   */
  public boolean allWorkersRegistered() {

    // if numberOfWorkers does not match the number of registered workers,
    // return false
    if (workers.size() != numberOfWorkers) {
      return false;
    }

    // if there is a gap in workerID sequence, return false
    // since workerIDs are sorted and they start from 0
    // checking the workerID of the last worker is sufficient
    if (workers.lastKey() != (numberOfWorkers - 1)) {
      return false;
    }

    return true;
  }

  /**
   * make sure that
   * all workers registered and their state is RUNNING
   * so that we can send a message to all
   */
  private boolean allWorkersRunning() {

    if (!allWorkersRegistered()) {
      return false;
    }

    // check the status of all workers, all have to be RUNNING
    for (WorkerWithState worker : workers.values()) {
      if (worker.getLastState() != JobMasterAPI.WorkerState.RUNNING) {
        return false;
      }
    }

    return true;
  }

  /**
   * worker RUNNING message received from all workers
   * if some workers may have already completed, that does not matter
   * the important thing is whether they have became RUNNING in the past
   */
  private boolean haveAllWorkersBecomeRunning() {
    if (numberOfWorkers != workers.size()) {
      return false;
    }

    for (WorkerWithState worker : workers.values()) {
      if (!worker.hasWorkerBecomeRunning()) {
        return false;
      }
    }

    return true;
  }

  /**
   * worker COMPLETED message received from all workers
   */
  private boolean allWorkersCompleted() {
    if (numberOfWorkers != workers.size()) {
      return false;
    }

    for (WorkerWithState worker : workers.values()) {
      if (!worker.hasWorkerCompleted()) {
        return false;
      }
    }

    return true;
  }

  /**
   * send WorkersJoined message to all workers and the driver
   */
  public void sendWorkersJoinedMessage() {

    LOG.info("Sending WorkersJoined messages ...");

    JobMasterAPI.WorkersJoined joinedMessage = constructWorkersJoinedMessage();

    // inform Driver if exist
    if (driver != null) {
      driver.allWorkersJoined(constructWorkerList());
    }

    // send the message to all workers
    for (Integer workerID : workers.keySet()) {
      rrServer.sendMessage(joinedMessage, workerID);
    }

  }

  /**
   * construct WorkersJoined message
   */
  private JobMasterAPI.WorkersJoined constructWorkersJoinedMessage() {

    JobMasterAPI.WorkersJoined.Builder joinedBuilder = JobMasterAPI.WorkersJoined.newBuilder()
        .setNumberOfWorkers(numberOfWorkers);

    for (WorkerWithState worker : workers.values()) {
      joinedBuilder.addWorker(worker.getWorkerInfo());
    }

    return joinedBuilder.build();
  }

  /**
   * construct WorkerList to send to Driver
   */
  private List<JobMasterAPI.WorkerInfo> constructWorkerList() {

    List<JobMasterAPI.WorkerInfo> workerList = new LinkedList<>();

    for (WorkerWithState worker : workers.values()) {
      workerList.add(worker.getWorkerInfo());
    }

    return workerList;
  }

}
