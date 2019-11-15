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

package edu.iu.dsc.tws.common.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.resource.ControllerContext;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IJobMasterListener;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * we assume each worker is assigned a unique ID outside of this class
 * If a worker joins with an ID that already exists in the group,
 * we assume that the worker is coming from failure. It is the same worker.
 * It is very important that there is no worker ID collusion among different workers in the same job
 * <p>
 * We create a persistent znode for the job.
 * Job znode is created with submitting client.
 * We create an ephemeral znode for each worker under the job znode.
 * <p>
 * Each worker znode keeps two pieces of data:
 * WorkerInfo object of the worker that created the ephemeral worker znode
 * status of this worker. Worker status changes during job execution and this field is updated.
 * <p>
 * When workers fail, and their znodes are deleted from ZooKeeper,
 * Their workerInfo objects will be also gone.
 * So, we keep a list of all joined workers in each worker locally.
 * When the worker comes back from a failure,
 * its WorkerInfo is updated in each local worker list.
 * <p>
 * When the job completes, job terminator deletes persistent job znode.
 * Sometimes, job terminator may not be invoked or it may fail before cleaning p job resources.
 * Therefore, when a job is submitted, it is important to check whether there is
 * any existing znode with the same name as the submitted job in ZooKeeper.
 * If so, job submission will fail and first job termination should be invoked.
 * <p>
 * we use a barrier to make all workers wait until the last worker arrives at the barrier point
 * we count the number of waiting workers by using a DistributedAtomicInteger
 */

public class ZKWorkerController implements IWorkerController, IWorkerStatusUpdater {

  public static final Logger LOG = Logger.getLogger(ZKWorkerController.class.getName());

  // number of workers in this job
  protected int numberOfWorkers;

  // name of this job
  protected String jobName;

  // WorkerInfo object for this worker
  private WorkerInfo workerInfo;

  // initial state of the worker
  private WorkerState initialState;

  // the client to connect to ZK server
  protected CuratorFramework client;

  // persistent ephemeral znode for this worker
  private PersistentNode workerEphemZNode;

  // children cache for events znode
  protected PathChildrenCache eventsChildrenCache;

  // config object
  protected Config config;
  protected String rootPath;

  // variables related to the barrier
  private DistributedAtomicInteger daiForBarrier;
  private DistributedBarrier barrier;

  // all workers in the job, including completed and failed ones
  private List<WorkerInfo> workers;

  // a flag to show whether all workers joined the job
  // Initially it is false. It becomes true when all workers joined the job.
  private boolean allJoined = false;

  // last event index at the time of restart, if restarted
  private int numberOfPastEvents;
  // put past events into this sorted map
  private TreeMap<Integer, JobMasterAPI.JobEvent> pastEvents;

  // synchronization object for waiting all workers to join the job
  private Object waitObject = new Object();

  // Inform worker failure events
  private IWorkerFailureListener failureListener;

  // Inform when all workers joined the job
  private IAllJoinedListener allJoinedListener;

  // Inform events related to job master
  private IJobMasterListener jobMasterListener;

  // some events may arrive after initializing the workerController but before
  // registering the listener,
  // we keep the last AllWorkersJoined event to deliver when there is no allJoinedListener
  private JobMasterAPI.AllWorkersJoined allJoinedEventCache;
  // we keep many fail events in the buffer to deliver later on
  private List<JobMasterAPI.JobEvent> failEventBuffer = new LinkedList<>();

  /**
   * Construct ZKWorkerController but not initialize yet
   */
  public ZKWorkerController(Config config,
                            String jobName,
                            int numberOfWorkers,
                            WorkerInfo workerInfo) {
    this.config = config;
    this.jobName = jobName;
    this.numberOfWorkers = numberOfWorkers;
    this.workerInfo = workerInfo;

    this.rootPath = ZKContext.rootNode(config);
    workers = new ArrayList<>(numberOfWorkers);
  }

  /**
   * Initialize this ZKWorkerController
   * Connect to the ZK server
   * create an ephemeral znode for this worker
   * set this worker info in the body of that node
   * worker status also goes into the body of that znode
   * initialState has to be either: WorkerState.STARTED or WorkerState.RESTARTED
   * <p>
   * The body of the worker znode will be updated as the status of worker changes
   * from STARTING, RUNNING, COMPLETED
   */
  public void initialize(WorkerState initState) throws Exception {

    this.initialState = initState;

    if (!(initState == WorkerState.STARTED || initState == WorkerState.RESTARTED)) {
      throw new Exception("initialState has to be either WorkerState.STARTED or "
          + "WorkerState.RESTARTED. Supplied value: " + initState);
    }

    try {
      String zkServerAddresses = ZKContext.serverAddresses(config);
      int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(config);
      client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);

      // update numberOfWorkers from jobZnode
      // with scaling up/down, it may have been changed
      JobAPI.Job job = ZKPersStateManager.readJobZNode(client, rootPath, jobName);
      numberOfWorkers = job.getNumberOfWorkers();

      // if this worker is started with scaling up, or
      // if it is coming from failure
      // we need to handle past events
      numberOfPastEvents = ZKEventsManager.getNumberOfPastEvents(client, rootPath, jobName);
      pastEvents = new TreeMap<>(Collections.reverseOrder());

      if (isRestarted()) {
        // delete ephemeral worker znode from previous run if any
        ZKEphemStateManager.removeEphemZNode(client, rootPath, jobName, workerInfo.getWorkerID());
      }

      // We cache children data for parent path.
      // So we will listen for all workers in the job
      String eventsDir = ZKUtils.eventsDir(rootPath, jobName);
      eventsChildrenCache = new PathChildrenCache(client, eventsDir, true);
      addEventsChildrenCacheListener(eventsChildrenCache);
      eventsChildrenCache.start();

      String barrierPath = ZKUtils.constructBarrierPath(rootPath, jobName);
      barrier = new DistributedBarrier(client, barrierPath);

      String daiPathForBarrier = ZKUtils.constructDaiPathForBarrier(rootPath, jobName);
      daiForBarrier = new DistributedAtomicInteger(client,
          daiPathForBarrier, new ExponentialBackoffRetry(1000, 3));

      createWorkerZnode();

      LOG.info("This worker: " + workerInfo.getWorkerID() + " initialized successfully.");

    } catch (Exception e) {
//      LOG.log(Level.SEVERE, "Exception when initializing ZKWorkerController", e);
      throw e;
    }
  }

  /**
   * create the znode for this worker
   */
  private void createWorkerZnode() {
    String workersEphemDir = ZKUtils.ephemDir(rootPath, jobName);
    String workerPath = ZKUtils.workerPath(workersEphemDir, workerInfo.getWorkerID());

    workerEphemZNode =
        ZKEphemStateManager.createWorkerZnode(client, rootPath, jobName, workerInfo.getWorkerID());
    workerEphemZNode.start();
    try {
      workerEphemZNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE,
          "Could not create worker ephemeral znode: " + workerPath, e);
      throw new RuntimeException("Could not create worker znode: " + workerPath, e);
    }

    String fullWorkerPath = workerEphemZNode.getActualPath();
    LOG.info("An ephemeral znode is created for this worker: " + fullWorkerPath);
  }

  private boolean isRestarted() {
    return initialState == WorkerState.RESTARTED;
  }

  /**
   * Update worker status with new state
   * return true if successful
   * <p>
   * Initially worker status is set as STARTED or RESTARTED.
   * Therefore, there is no need to call this method after starting this IWorkerController
   * This method should be called to change worker status to COMPLETED, FAILED, etc.
   */
  @Override
  public boolean updateWorkerStatus(WorkerState newStatus) {

    return ZKPersStateManager.updateWorkerStatus(client, rootPath, jobName, workerInfo, newStatus);
  }

  @Override
  public WorkerState getWorkerStatusForID(int id) {
    String workersPersDir = ZKUtils.persDir(rootPath, jobName);
    WorkerWithState workerWS = ZKPersStateManager.getWorkerWithState(client, workersPersDir, id);
    if (workerWS != null) {
      return workerWS.getState();
    }

    return null;
  }

  @Override
  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  @Override
  public WorkerInfo getWorkerInfoForID(int id) {

    // get it from local cache if allJoined,
    if (allJoined) {
      return workers.stream()
          .filter(wInfo -> wInfo.getWorkerID() == id)
          .findFirst()
          .orElse(null);
    }

    String workersPersDir = ZKUtils.persDir(rootPath, jobName);
    WorkerWithState workerWS = ZKPersStateManager.getWorkerWithState(client, workersPersDir, id);
    if (workerWS != null) {
      return workerWS.getInfo();
    }

    return null;
  }

  @Override
  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  @Override
  public List<WorkerInfo> getJoinedWorkers() {

    List<WorkerWithState> workersWithState =
        ZKPersStateManager.getWorkers(client, rootPath, jobName);

    return workersWithState
        .stream()
        .filter(workerWithState -> workerWithState.running())
        .map(workerWithState -> workerWithState.getInfo())
        .collect(Collectors.toList());
  }

  @Override
  public List<WorkerInfo> getAllWorkers() throws TimeoutException {
    // if all workers already joined, return the workers list
    if (allJoined) {
      return cloneWorkers();
    }

    // wait until all workers joined or time limit is reached
    long timeLimit = ControllerContext.maxWaitTimeForAllToJoin(config);
    long startTime = System.currentTimeMillis();

    long delay = 0;
    while (delay < timeLimit) {
      synchronized (waitObject) {
        try {
          waitObject.wait(timeLimit - delay);

          // proceeding with notification or timeout
          if (allJoined) {
            return cloneWorkers();
          } else {
            throw new TimeoutException("Not all workers joined the job on the given time limit: "
                + timeLimit + "ms.");
          }

        } catch (InterruptedException e) {
          delay = System.currentTimeMillis() - startTime;
        }
      }
    }

    if (allJoined) {
      return cloneWorkers();
    } else {
      throw new TimeoutException("Not all workers joined the job on the given time limit: "
          + timeLimit + "ms.");
    }
  }

  protected List<WorkerInfo> cloneWorkers() {

    List<WorkerInfo> clone = new LinkedList<>();
    for (WorkerInfo info : workers) {
      clone.add(info);
    }
    return clone;
  }

  /**
   * remove the workerInfo for the given ID if it exists
   */
  private void updateWorkerInfo(WorkerInfo info) {
    workers.removeIf(wInfo -> wInfo.getWorkerID() == info.getWorkerID());
    workers.add(info);
  }



  /**
   * add a single IWorkerFailureListener
   * if additional IWorkerFailureListener tried to be added,
   * do not add and return false
   */
  public boolean addFailureListener(IWorkerFailureListener iWorkerFailureListener) {
    if (this.failureListener != null) {
      return false;
    }

    this.failureListener = iWorkerFailureListener;

    // if failEventBuffer is not null, deliver all messages in a new thread
    if (!failEventBuffer.isEmpty()) {
      Executors.newSingleThreadExecutor().execute(new Runnable() {
        @Override
        public void run() {
          for (JobMasterAPI.JobEvent jobEvent: failEventBuffer) {
            if (jobEvent.hasRestarted()) {
              failureListener.restarted(jobEvent.getRestarted().getWorkerInfo());
              LOG.fine("FAILED event delivered from cache.");
            } else if (jobEvent.hasFailed()) {
              failureListener.failed(jobEvent.getFailed().getWorkerID());
              LOG.fine("RESTARTED event delivered from cache.");
            }
          }
        }
      });
    }

    return true;
  }

  /**
   * add a single IAllJoinedListener
   * if additional IAllJoinedListener tried to be added,
   * do not add and return false
   */
  public boolean addAllJoinedListener(IAllJoinedListener iAllJoinedListener) {
    if (allJoinedListener != null) {
      return false;
    }

    allJoinedListener = iAllJoinedListener;

    // if allJoinedEventToDeliver is not null, deliver that message in a new thread
    if (allJoinedEventCache != null) {
      Executors.newSingleThreadExecutor().execute(new Runnable() {
        @Override
        public void run() {
          allJoinedListener.allWorkersJoined(allJoinedEventCache.getWorkerInfoList());
          LOG.fine("AllWorkersJoined event delivered from cache.");
        }
      });
    }

    return true;
  }

  /**
   * TODO: not implemented yet
   * add a single IJobMasterListener
   * if additional IJobMasterListener tried to be added,
   * do not add and return false
   */
  public boolean addJobMasterListener(IJobMasterListener iJobMasterListener) {
    if (jobMasterListener != null) {
      return false;
    }

    jobMasterListener = iJobMasterListener;
    return true;
  }

  /**
   * Get current list of workers
   * This list does not have the workers that have failed or already completed
   */
  public List<WorkerInfo> getCurrentWorkers() {

    List<WorkerWithState> workersWithState =
        ZKPersStateManager.getWorkers(client, rootPath, jobName);

    return workersWithState
        .stream()
        .filter(workerWithState -> workerWithState.startedOrCompleted())
        .map(workerWithState -> workerWithState.getInfo())
        .collect(Collectors.toList());
  }

  /**
   * get number of current workers in the job as seen from this worker
   */
  public int getNumberOfCurrentWorkers() throws Exception {

    String workersEphemDir = ZKUtils.ephemDir(rootPath, jobName);
    int size = -1;
    try {
      size = client.getChildren().forPath(workersEphemDir).size();
    } catch (Exception e) {
      throw e;
    }
    return size;
  }

  private void addEventsChildrenCacheListener(PathChildrenCache cache) {
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        switch (event.getType()) {
          case CHILD_ADDED:
            eventPublished(event);
            break;

          default:
            // nothing to do
        }
      }
    };
    cache.getListenable().addListener(listener);
  }

  /**
   * when a new znode added to this job znode,
   * take necessary actions
   */
  private void eventPublished(PathChildrenCacheEvent event) {

    String eventPath = event.getData().getPath();
    int eventIndex = ZKUtils.getWorkerIDFromPersPath(eventPath);

    byte[] eventData = event.getData().getData();
    JobMasterAPI.JobEvent jobEvent;
    try {
      jobEvent = ZKEventsManager.decodeJobEvent(eventData);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Could not decode received JobEvent from the path: " + eventPath, e);
      return;
    }

    // if there are already events when the worker has started, handle those
    if (eventIndex < numberOfPastEvents) {

      // ignore all past events for a worker that is not restarted
      if (!isRestarted()) {
        return;
      }

      // if the worker is restarted, put all events into the map object
      // we will use the latest allJoined event only from the past events
      pastEvents.put(eventIndex, jobEvent);
      if (pastEvents.size() == numberOfPastEvents) {
        processPastEvents();
      }

      return;
    }

    if (jobEvent.hasAllJoined()) {
      processAllJoinedEvent(jobEvent);
      return;
    }

    if (jobEvent.hasFailed()) {
      JobMasterAPI.WorkerFailed workerFailed = jobEvent.getFailed();
      // if the event is about this worker, ignore it
      if (workerFailed.getWorkerID() == workerInfo.getWorkerID()) {
        return;
      }

      LOG.info(String.format("Worker[%s] FAILED. ", workerFailed.getWorkerID()));

      if (failureListener != null) {
        failureListener.failed(workerFailed.getWorkerID());
      } else {
        failEventBuffer.add(jobEvent);
      }
    }

    if (jobEvent.hasRestarted()) {

      JobMasterAPI.WorkerRestarted workerRestarted = jobEvent.getRestarted();
      // if the event is about this worker, ignore it
      if (workerRestarted.getWorkerInfo().getWorkerID() == workerInfo.getWorkerID()) {
        return;
      }

      LOG.info(String.format("Worker[%s] RESTARTED.",
          workerRestarted.getWorkerInfo().getWorkerID()));

      updateWorkerInfo(workerRestarted.getWorkerInfo());

      if (failureListener != null) {
        failureListener.restarted(workerRestarted.getWorkerInfo());
      } else {
        failEventBuffer.add(jobEvent);
      }
    }

  }

  /**
   * look for an AllJoined event among past events,
   * if there is, process the one with highest index
   * ignore all other past events
   * PastEvents are sorted in reverse order, so we process the first one we find
   */
  private void processPastEvents() {
    for (Map.Entry<Integer, JobMasterAPI.JobEvent> entry: pastEvents.entrySet()) {
      if (entry.getValue().hasAllJoined()) {
        LOG.info("AllWorkersJoined event from past events. Event index: " + entry.getKey());
        processAllJoinedEvent(entry.getValue());
        return;
      }
    }
  }

  private void processAllJoinedEvent(JobMasterAPI.JobEvent jobEvent) {

    JobMasterAPI.AllWorkersJoined allJoinedEvent = jobEvent.getAllJoined();
    workers.clear();
    workers.addAll(allJoinedEvent.getWorkerInfoList());
    allJoined = true;

    synchronized (waitObject) {
      waitObject.notify();
    }

    if (allJoinedListener != null) {
      allJoinedListener.allWorkersJoined(allJoinedEvent.getWorkerInfoList());
    } else {
      allJoinedEventCache = allJoinedEvent;
    }
  }

  /**
   * try to increment the daiForBarrier
   * try 100 times if fails
   */
  private boolean incrementBarrierDAI(int tryCount, long timeLimitMilliSec) {

    if (tryCount == 100) {
      return false;
    }

    try {
      AtomicValue<Integer> incremented = daiForBarrier.increment();
      if (incremented.succeeded()) {
        LOG.fine("DistributedAtomicInteger for Barrier increased to: " + incremented.postValue());

        // if this is the last worker to enter, remove the barrier and let all workers be released
        if (incremented.postValue() == numberOfWorkers) {
          barrier.removeBarrier();
          // clear the value to zero, so that new barrier can be used
          daiForBarrier.forceSet(0);
          return true;

          // if this is not the last worker, set the barrier and wait
        } else {
          barrier.setBarrier();
          return barrier.waitOnBarrier(timeLimitMilliSec, TimeUnit.MILLISECONDS);
        }

      } else {
        return incrementBarrierDAI(tryCount + 1, timeLimitMilliSec);
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Failed to increment the DistributedAtomicInteger for Barrier. "
          + "Will try again ...", e);
      return incrementBarrierDAI(tryCount + 1, timeLimitMilliSec);
    }
  }

  /**
   * we use a DistributedAtomicInteger to count the number of workers
   * that have reached to the barrier point.
   * <p>
   * Last worker to call this method increases the DistributedAtomicInteger,
   * removes the barrier and lets all previous waiting workers be released.
   * <p>
   * Other workers to call this method increase the DistributedAtomicInteger,
   * enable the barrier by calling setBarrier method and wait.
   * <p>
   * It is enough to call setBarrier method by only the first worker,
   * however, it does not harm calling by many workers.
   * <p>
   * If we let only the first worker to set the barrier with setBarrier method,
   * then, the second worker may call this method after the dai is increased
   * but before the setBarrier method is called. To prevent this,
   * we may need to use a distributed InterProcessMutex.
   * So, instead of using a distributed InterProcessMutex, we call this method many times.
   * <p>
   * We reset the value of DistributedAtomicInteger to zero after all workers reached the barrier
   * This lets barrier to be used again even if the job is scaled up/down
   * <p>
   * This method may be called many times during a computation.
   * <p>
   * If the job is scaled during some workers are waiting on a barrier,
   * The barrier does not work, since the numberOfWorkers changes
   * <p>
   * if timeout is reached, throws TimeoutException.
   */
  @Override
  public void waitOnBarrier() throws TimeoutException {

    boolean allArrived = incrementBarrierDAI(0, ControllerContext.maxWaitTimeOnBarrier(config));
    if (!allArrived) {

      // clear the value to zero, so that new barrier can be used
      try {
        daiForBarrier.forceSet(0);
      } catch (Exception e) {
        LOG.severe("DistributedAtomicInteger value can not be set to zero.");
      }

      throw new TimeoutException("All workers have not arrived at the barrier on the time limit: "
          + ControllerContext.maxWaitTimeOnBarrier(config) + "ms.");
    }
  }


  /**
   * close all local entities.
   */
  public void close() {
    CloseableUtils.closeQuietly(workerEphemZNode);
    CloseableUtils.closeQuietly(eventsChildrenCache);
  }

}
