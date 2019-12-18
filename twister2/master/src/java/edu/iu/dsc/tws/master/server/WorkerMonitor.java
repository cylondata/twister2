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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;


import edu.iu.dsc.tws.api.driver.IDriver;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.zk.WorkerWithState;
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
public class WorkerMonitor implements MessageHandler {

  private static final Logger LOG = Logger.getLogger(WorkerMonitor.class.getName());

  private JobMaster jobMaster;
  private RRServer rrServer;
  private DashboardClient dashClient;
  private IDriver driver;
  // whether this is a fault tolerant job
  private boolean faultTolerant;

  // upto date status of the job
  private JobState jobState;

  // a flag to show whether all expected workers has already joined
  private boolean allJoined = false;

  // a flag to show whether AllJoined event published to the driver
  // it is published when the driver exists, all workers joined and all workers connected
  // it is initially false and becomes true after it is published
  // it also becomes false after scaling up and becomes true
  // when AllJoined event published to the driver again
  private boolean publishedAllJoinedToDriver = false;

  /**
   * numberOfWorkers in the job is tracked by this variable
   * all other classes in job master should get the upto-date numberOfWorkers from this variable
   * if is updated in the case of scale up and down
   */
  private int numberOfWorkers;

  private ConcurrentSkipListMap<Integer, WorkerWithState> workers;

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
    this.jobState = JobState.STARTING;

    workers = new ConcurrentSkipListMap<>();
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

  public JobMasterAPI.WorkerInfo getWorkerInfoForID(int id) {
    WorkerWithState workerWithState = workers.get(id);
    if (workerWithState == null) {
      return null;
    } else {
      return workerWithState.getInfo();
    }
  }

  /**
   * get the list of workerIDs sorted
   */
  public List<Integer> getWorkerIDs() {
    return workers
        .values()
        .stream()
        .map(wws -> wws.getWorkerID())
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * get the list of workerIDs sorted
   */
  public List<JobMasterAPI.WorkerInfo> getWorkerInfoList() {
    return workers
        .values()
        .stream()
        .map(wws -> wws.getInfo())
        .collect(Collectors.toList());
  }

  /**
   * return true if there is a worker with the given workerID
   */
  public boolean existWorker(int workerID) {
    return workers.containsKey(workerID);
  }

  public boolean isAllJoined() {
    return allJoined;
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
   * new worker joins for the first time
   * returns null if the join is successful,
   * otherwise, it returns an explanation for the failure
   */
  public String started(WorkerWithState workerWithState) {

    // if the workerID of joined worker is higher than numberOfWorkers in the job,
    // log a warning message.
    // in a possible but unlikely case: when workers scaled up,
    // new workers might join before the scaler informs WorkerMonitor
    // in that case, numberOfWorkers would be smaller than workerID
    if (workerWithState.getWorkerID() >= numberOfWorkers) {
      String warnMessage = String.format(
          "A worker joined but its workerID[%s] is higher than numberOfWorkers[%s]. "
              + "If this is not because of scaling up, it seems problematic. Joined worker: %s",
          workerWithState.getWorkerID(), numberOfWorkers, workerWithState.getInfo());
      LOG.warning(warnMessage);
    }

    if (existWorker(workerWithState.getWorkerID())) {
      String failMessage = "There is an already registered worker with workerID: "
          + workerWithState.getWorkerID();
      LOG.severe(failMessage);
      return failMessage;
    }

    // if it is a regular join event
    // add the worker to worker list
    workers.put(workerWithState.getWorkerID(), workerWithState);
    LOG.info("Worker: " + workerWithState.getWorkerID() + " joined the job.");

    // send worker registration message to dashboard
    if (dashClient != null) {
      dashClient.registerWorker(workerWithState.getInfo(), workerWithState.getState());
    }

    handleAllJoined();
    return null;
  }

  /**
   * if the worker is coming from failure
   */
  public String restarted(WorkerWithState workerWithState) {

    // if the workerID of joined worker is higher than numberOfWorkers in the job,
    // log a warning message.
    // in a possible but unlikely case: when workers scaled up,
    // new workers might join before the scaler informs WorkerMonitor
    // in that case, numberOfWorkers would be smaller than workerID
    if (workerWithState.getWorkerID() >= numberOfWorkers) {
      String warnMessage = String.format(
          "A worker joined but its workerID[%s] is higher than numberOfWorkers[%s]. "
              + "If this is not because of scaling up, it seems problematic. Joined worker: %s",
          workerWithState.getWorkerID(), numberOfWorkers, workerWithState.getInfo());
      LOG.warning(warnMessage);
    }

    // send worker registration message to dashboard
    if (dashClient != null) {
      dashClient.registerWorker(workerWithState.getInfo(), workerWithState.getState());
    }

    // if this is not a fault tolerant job, we terminate the job with failure
    // because, this worker has failed previously and it is coming from failure
    if (!faultTolerant) {
      jobState = JobState.FAILED;
      String failMessage =
          String.format("worker[%s] is coming from failure in NON-FAULT TOLERANT job. "
              + "Terminating the job.", workerWithState.getWorkerID());
      LOG.info(failMessage);
      jobMaster.completeJob(JobState.FAILED);
      return failMessage;
    }

    if (!existWorker(workerWithState.getWorkerID())) {
      LOG.warning(String.format("The worker[%s] that has not joined the job yet, tries to rejoin. "
              + "Ignoring this event.",
          workerWithState.getWorkerID()));
    }

    // update workerInfo and its status in the worker list
    workers.put(workerWithState.getWorkerID(), workerWithState);
    LOG.info("WorkerID: " + workerWithState.getWorkerID() + " rejoined from failure.");

    handleAllJoined();

    // TODO inform checkpoint master
    return null;
  }

  /**
   * handle allJoined after started and restarted events
   */
  private void handleAllJoined() {

    if (!allJoined && allWorkersJoined()) {
      allJoined = true;

      // inform Driver if exist and allConnected
      informDriverForAllJoined();

      // if the job is becoming all joined for the first time, inform dashboard
      if (jobState == JobState.STARTING) {
        jobState = JobState.STARTED;

        if (dashClient != null) {
          dashClient.jobStateChange(JobState.STARTED);
        }
      }
    }
  }

  /**
   * called when a worker COMPLETED the job
   */
  public void completed(int workerID) {
    workers.get(workerID).setState(JobMasterAPI.WorkerState.COMPLETED);
    LOG.info("Worker:" + workerID + " COMPLETED.");

    // send worker state change message to dashboard
    if (dashClient != null) {
      dashClient.workerStateChange(workerID, JobMasterAPI.WorkerState.COMPLETED);
    }

    // check whether all workers completed
    // if so, stop the job master
    // if all workers have completed, no need to send the response message back to the client
    if (allWorkersCompleted()) {
      jobState = JobState.COMPLETED;
      LOG.info("All " + numberOfWorkers + " workers COMPLETED. Terminating the job.");
      jobMaster.completeJob(JobState.COMPLETED);
    }
  }

  /**
   * called when a worker FAILED
   */
  public void failed(int workerID) {

    WorkerWithState failedWorker = workers.get(workerID);
    if (failedWorker == null) {
      LOG.warning("The worker[" + workerID + "] that hos not joined the job failed. "
          + "Ignoring this event.");
      return;
    }

    failedWorker.setState(JobMasterAPI.WorkerState.FAILED);
    LOG.info("Worker: " + workerID + " FAILED.");

    // send worker state change message to dashboard
    if (dashClient != null) {
      dashClient.workerStateChange(workerID, JobMasterAPI.WorkerState.FAILED);
    }

    // TODO: test whether this works
    // if this is a non-fault tolerant job, job needs to be terminated
    if (!faultTolerant) {
      jobState = JobState.FAILED;
      LOG.info("A worker failed in a NON-FAULT TOLERANT job. Terminating the job.");
      jobMaster.completeJob(JobState.FAILED);
    }
  }

  public void workersScaledDown(int instancesRemoved) {

    // modify numberOfWorkers
    numberOfWorkers -= instancesRemoved;

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

    // let either controller to know
    if (jobMaster.getZkMasterController() != null) {
      jobMaster.getZkMasterController().jobScaledDown(numberOfWorkers);
    } else if (jobMaster.getWorkerHandler() != null) {
      jobMaster.getWorkerHandler().workersScaledDown(instancesRemoved);
    }

    // send Scale message to the dashboard
    int change = 0 - instancesRemoved;
    if (dashClient != null) {
      dashClient.scaledWorkers(change, numberOfWorkers, killedWorkers);
    }
  }

  public void workersScaledUp(int instancesAdded) {

    allJoined = false;
    publishedAllJoinedToDriver = false;

    // keep previous numberOfWorkers and update numberOfWorkers with new value
    numberOfWorkers += instancesAdded;
    if (jobMaster.getZkMasterController() != null) {
      jobMaster.getZkMasterController().jobScaledUp(numberOfWorkers);
    }
    if (jobMaster.getWorkerHandler() != null) {
      jobMaster.getWorkerHandler().workersScaledUp(instancesAdded);
    }

    // in the case of very unlikely but possible scenario
    // all scaled up workers may be already joined
    // in that case, publish the event
    if (allWorkersJoined()) {
      allJoined = true;

      if (jobMaster.getZkMasterController() != null) {
        jobMaster.getZkMasterController().publishAllJoined();
      } else {
        jobMaster.getWorkerHandler().sendWorkersJoinedMessage();
      }

      informDriverForAllJoined();
    }

    // send Scaled message to the dashboard
    if (dashClient != null) {
      dashClient.scaledWorkers(instancesAdded, numberOfWorkers, new LinkedList<Integer>());
    }
  }

  /**
   * when the job master restarts, it adds already joined workers with this method.
   * returns true if allJoined becomes true
   */
  public boolean addJoinedWorkers(List<WorkerWithState> joinedWorkers) {
    for (WorkerWithState wws : joinedWorkers) {
      workers.put(wws.getWorkerID(), wws);
    }

    if (workers.size() == numberOfWorkers && allWorkersJoined()) {
      allJoined = true;
      jobState = JobState.STARTED;

      LOG.info("All workers have already joined, before the job master restarted.");
      return true;
    }

    return false;
  }

  /**
   * inform the driver on restarts if all workers already joined
   */
  public void informDriverForAllJoined() {
    if (allJoined
        && driver != null
        && !publishedAllJoinedToDriver
        && jobMaster.getWorkerHandler().isAllConnected()) {
      driver.allWorkersJoined(getWorkerInfoList());
      publishedAllJoinedToDriver = true;
    }
  }

  public boolean broadcastMessage(Message message) {

    JobMasterAPI.DriverMessage driverMessage =
        JobMasterAPI.DriverMessage.newBuilder()
            .setData(Any.pack(message).toByteString())
            .build();

    // if all workers are not running
    // send a failure response message to the driver
    // do not send the broadcast message to any workers
    if (!allWorkersRunning()) {
      LOG.warning("Could not send the broadcast message to all workers, "
          + "since not all are currenty running.");
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
      } else if (worker.getState() != JobMasterAPI.WorkerState.RESTARTED
          && worker.getState() != JobMasterAPI.WorkerState.STARTED) {
        LOG.warning("Can not deliver the message to workerID[" + workerID + "]. "
            + "Its status: " + worker.getState());
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
   * if all workers are in one of these states: STARTED, RESTARTED or COMPLETED.
   * return true
   * <p>
   * This is used to send allWorkersJoined message
   * We omit started but failed workers
   * We include started but completed workers
   */
  public boolean allWorkersJoined() {

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

    // check the status of all workers, all have to be either STARTED, RESTARTED, COMPLETED
    for (WorkerWithState worker : workers.values()) {
      if (!worker.startedOrCompleted()) {
        return false;
      }
    }

    return true;
  }

  /**
   * make sure that
   * all workers started or restarted
   * so that we can send a message to all
   */
  private boolean allWorkersRunning() {

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

    // check the status of all workers, all have to be either STARTED, RESTARTED
    for (WorkerWithState worker : workers.values()) {
      if (!worker.running()) {
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
      if (!worker.completed()) {
        return false;
      }
    }

    return true;
  }

}
