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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.resource.IBarrierListener;
import edu.iu.dsc.tws.common.zk.WorkerWithState;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKEphemStateManager;
import edu.iu.dsc.tws.common.zk.ZKEventsManager;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.JobMasterState;

//TODO: publish jm restarted to dashboard

public class ZKMasterController {
  public static final Logger LOG = Logger.getLogger(ZKMasterController.class.getName());

  // number of workers in this job
  protected int numberOfWorkers;
  protected String jobID;

  // config object
  protected Config config;

  // zk paths for the job
  protected String rootPath;
  protected String persDir;
  protected String ephemDir;

  // Job Master IP address
  private String jmAddress;

  // the client to connect to ZK server
  protected CuratorFramework client;

  // children cache for ephemeral worker znodes in the job for worker joins and failures
  protected PathChildrenCache ephemChildrenCache;

  // children cache for persistent worker znodes for watching status changes
  protected PathChildrenCache persChildrenCache;

  // children cache for default barrier directory for determining barrier arrival of all workers
  protected PathChildrenCache defaultBarrierCache;

  // children cache for default barrier directory for determining barrier arrival of all workers
  protected PathChildrenCache initBarrierCache;

  // keep the list of workers at the barriers
  // all workers must arrive at the barrier
  // and they all have to be removed after allArrived event published
  private TreeSet<Integer> workersAtDefault = new TreeSet<>();
  private TreeSet<Integer> toBeRemovedWorkersFromDefault = new TreeSet<>();
  private TreeSet<Integer> workersAtInit = new TreeSet<>();
  private TreeSet<Integer> toBeRemovedWorkersFromInit = new TreeSet<>();

  // persistent ephemeral znode for the job master for workers to watch the job master
  private PersistentNode masterEphemZNode;

  // list of scaled down workers
  // when the job is scaled down, we populate this list
  // we remove each ID when we received worker znode removed event
  // with this list, we distinguish worker failures and worker deletion by scaling down
  private List<Integer> scaledDownWorkers = new LinkedList<>();

  private WorkerMonitor workerMonitor;

  private IBarrierListener barrierListener;

  public ZKMasterController(Config config,
                            String jobID,
                            int numberOfWorkers,
                            String jmAddress,
                            WorkerMonitor workerMonitor,
                            IBarrierListener barrierListener) {
    this.config = config;
    this.jobID = jobID;
    this.numberOfWorkers = numberOfWorkers;
    this.jmAddress = jmAddress;
    this.workerMonitor = workerMonitor;
    this.barrierListener = barrierListener;

    rootPath = ZKContext.rootNode(config);
    persDir = ZKUtils.persDir(rootPath, jobID);
    ephemDir = ZKUtils.ephemDir(rootPath, jobID);
  }

  /**
   * initialize ZKMasterController,
   * create znode children caches for job master to watch proper events
   */
  public void initialize(JobMasterState initialState) throws Twister2Exception {

    if (!(initialState == JobMasterState.JM_STARTED
        || initialState == JobMasterState.JM_RESTARTED)) {
      throw new Twister2Exception("initialState has to be either JobMasterState.JM_STARTED or "
          + "JobMasterState.JM_RESTARTED. Supplied value: " + initialState);
    }

    try {
      String zkServerAddresses = ZKContext.serverAddresses(config);
      int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(config);
      client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);

      // update numberOfWorkers from jobZnode
      // with scaling up/down, it may have been changed
      if (initialState == JobMasterState.JM_RESTARTED) {

        initRestarting();

      } else {

        // We listen for join/remove events for ephemeral children
        ephemChildrenCache = new PathChildrenCache(client, ephemDir, true);
        addEphemChildrenCacheListener(ephemChildrenCache);
        ephemChildrenCache.start();

        // We listen for status updates for persistent path
        persChildrenCache = new PathChildrenCache(client, persDir, true);
        addPersChildrenCacheListener(persChildrenCache);
        persChildrenCache.start();
      }

      // We listen for status updates for the default barrier path
      String defaultBarrierDir = ZKUtils.defaultBarrierDir(rootPath, jobID);
      defaultBarrierCache = new PathChildrenCache(client, defaultBarrierDir, true);
      addBarrierChildrenCacheListener(defaultBarrierCache, workersAtDefault,
          toBeRemovedWorkersFromDefault, JobMasterAPI.AllArrivedOnBarrier.BarrierType.DEFAULT);
      defaultBarrierCache.start();

      // We listen for status updates for the init barrier path
      String initBarrierDir = ZKUtils.initBarrierDir(rootPath, jobID);
      initBarrierCache = new PathChildrenCache(client, initBarrierDir, true);
      addBarrierChildrenCacheListener(initBarrierCache, workersAtInit, toBeRemovedWorkersFromInit,
          JobMasterAPI.AllArrivedOnBarrier.BarrierType.INIT);
      initBarrierCache.start();

      // TODO: we nay need to create ephemeral job master znode so that
      //   workers can know when jm fails
      //   createJobMasterZnode(initialState);

      LOG.info("Job Master: " + jmAddress + " initialized successfully.");

    } catch (Twister2Exception e) {
      throw e;
    } catch (Exception e) {
      throw new Twister2Exception("Exception when initializing ZKMasterController.", e);
    }
  }

  /**
   * initialize JM when it is coming from failure
   * todo: we need to implement jm-restart-resistant barriers
   *       barriers not tested for jm restart
   * @throws Exception
   */
  private void initRestarting() throws Exception {
    LOG.info("Job Master restarting .... ");

    // build the cache
    // we will not get events for the past worker joins/fails
    ephemChildrenCache = new PathChildrenCache(client, ephemDir, true);
    addEphemChildrenCacheListener(ephemChildrenCache);
    ephemChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

    List<ChildData> joinedWorkerZnodes = ephemChildrenCache.getCurrentData();
    LOG.info("Initially existing workers: " + joinedWorkerZnodes.size());

    // We listen for status updates for persistent path
    persChildrenCache = new PathChildrenCache(client, persDir, true);
    addPersChildrenCacheListener(persChildrenCache);
    persChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

    // get all joined workers and provide them to workerMonitor
    List<WorkerWithState> joinedWorkers = new LinkedList<>();
    for (ChildData child : joinedWorkerZnodes) {
      String fullPath = child.getPath();
      int workerID = ZKUtils.getWorkerIDFromEphemPath(fullPath);

      WorkerWithState workerWithState = getWorkerWithState(workerID);
      if (workerWithState != null) {
        joinedWorkers.add(workerWithState);
      } else {
        LOG.severe("worker[" + fullPath + "] added, but its data can not be retrieved.");
      }
    }

    // publish jm restarted event
    publishJobMasterRestarted();

    // if all workers joined and allJoined event has not been published, publish it
    boolean allJoined = workerMonitor.addJoinedWorkers(joinedWorkers);
    if (allJoined && !allJoinedPublished()) {
      LOG.info("Publishing AllJoined event when restarting, since it is not previously published.");
      publishAllJoined();
    }
  }

  /**
   * check whether allJoined event published previously for current number of workers
   */
  private boolean allJoinedPublished() throws Exception {

    TreeMap<Integer, JobMasterAPI.JobEvent> events =
        ZKEventsManager.getAllEvents(client, rootPath, jobID);

    for (JobMasterAPI.JobEvent event : events.values()) {
      // allJoined event with highest index, must have the same number of workers
      // if so, allJoined event already published, otherwise not published yet
      if (event.hasAllJoined()) {

        if (event.getAllJoined().getNumberOfWorkers() == numberOfWorkers) {
          return true;
        }
        return false;
      }
    }

    return false;
  }

  /**
   * this method invoked by WorkerMonitor, when the job is scaled up
   */
  public void jobScaledUp(int newNumberOfWorkers) {
    this.numberOfWorkers = newNumberOfWorkers;
  }

  /**
   * this method invoked by WorkerMonitor, when the job is scaled down
   */
  public void jobScaledDown(int newNumberOfWorkers) {
    scaledDownWorkers = new LinkedList<>();
    for (int i = newNumberOfWorkers; i < numberOfWorkers; i++) {
      scaledDownWorkers.add(i);
    }

    this.numberOfWorkers = newNumberOfWorkers;
  }

  /**
   * create ephemeral znode for the job master
   */
  private void createJMEphemZnode(JobMasterState initialState) {
    String jmPath = ZKUtils.jmEphemPath(rootPath, jobID);

    // put masterAddress and its state into znode body
    byte[] jmZnodeBody = ZKUtils.encodeJobMasterZnode(jmAddress, initialState.getNumber());
    masterEphemZNode = ZKUtils.createPersistentEphemeralZnode(jmPath, jmZnodeBody);
    masterEphemZNode.start();
    try {
      masterEphemZNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE,
          "Could not create job master znode.", e);
      throw new RuntimeException("Could not create job master znode", e);
    }

    String fullPath = masterEphemZNode.getActualPath();
    LOG.info("An ephemeral znode is created for the job master: " + fullPath);
  }

  /**
   * create the listener for ephemeral worker znodes to determine worker joins and failures
   */
  private void addEphemChildrenCacheListener(PathChildrenCache cache) {
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        switch (event.getType()) {
          case CHILD_ADDED:
            workerZnodeAdded(event);
            break;

          case CHILD_REMOVED:
            workerZnodeRemoved(event);
            break;

          default:
            // nothing to do
        }
      }
    };
    cache.getListenable().addListener(listener);
  }

  /**
   * create the listener for persistent worker znodes to determine worker status changes
   */
  private void addPersChildrenCacheListener(PathChildrenCache cache) {
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        switch (event.getType()) {

          case CHILD_UPDATED:
            childZnodeUpdated(event);
            break;

          default:
            // nothing to do
        }
      }
    };
    cache.getListenable().addListener(listener);
  }

  /**
   * when a new worker znode added to the ephemeral job directory,
   * take necessary actions
   */
  private void workerZnodeAdded(PathChildrenCacheEvent event) {

    boolean initialAllJoined = workerMonitor.isAllJoined();

    String addedChildPath = event.getData().getPath();
    int workerID = ZKUtils.getWorkerIDFromEphemPath(addedChildPath);
    WorkerWithState workerWithState = getWorkerWithState(workerID);
    if (workerWithState == null) {
      LOG.severe("worker[" + workerID + "] added, but its data can not be retrieved.");
      return;
    }

    // if the status of joining worker is RESTARTED, it is coming from failure
    if (workerWithState.getState() == JobMasterAPI.WorkerState.RESTARTED) {

      workerMonitor.restarted(workerWithState);
      publishWorkerRestarted(workerWithState);

    } else if (workerWithState.getState() == JobMasterAPI.WorkerState.STARTED) {

      workerMonitor.started(workerWithState);

      // a worker joined with initial state that is not acceptable
    } else {
      LOG.warning("Following worker joined with initial state of " + workerWithState.getState()
          + "Something must be wrong. Ignoring this event. WorkerInfo: "
          + workerWithState.getInfo());
      return;
    }

    // if this is the last worker to join the job and currently all workers joined the job,
    // let all workers know that all joined
    if (!initialAllJoined && workerMonitor.isAllJoined()) {
      publishAllJoined();
    }
  }

  /**
   * get WorkerWithState from the local cache if exists,
   * otherwise, get it from the server
   */
  private WorkerWithState getWorkerWithState(int workerID) {
    String workerPersPath = ZKUtils.workerPath(persDir, workerID);
    ChildData znodeBody = persChildrenCache.getCurrentData(workerPersPath);
    if (znodeBody != null) {
      return WorkerWithState.decode(znodeBody.getData());
    }

    try {
      return ZKPersStateManager.getWorkerWithState(client, workerPersPath);
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }
  }

  public void publishWorkerRestarted(WorkerWithState workerWithState) {
    // generate en event and inform all other workers
    JobMasterAPI.WorkerRestarted workerRestarted = JobMasterAPI.WorkerRestarted.newBuilder()
        .setWorkerInfo(workerWithState.getInfo())
        .build();

    JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
        .setRestarted(workerRestarted)
        .build();
    try {
      ZKEventsManager.publishEvent(client, rootPath, jobID, jobEvent);
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  public void publishWorkerFailed(int failedID) {
    JobMasterAPI.WorkerFailed workerFailed = JobMasterAPI.WorkerFailed.newBuilder()
        .setWorkerID(failedID)
        .build();

    JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
        .setFailed(workerFailed)
        .build();

    try {
      ZKEventsManager.publishEvent(client, rootPath, jobID, jobEvent);
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  /**
   * generate and publish all joined event
   */
  public void publishAllJoined() {
    List<JobMasterAPI.WorkerInfo> workers = workerMonitor.getWorkerInfoList();

    JobMasterAPI.AllWorkersJoined allWorkersJoined = JobMasterAPI.AllWorkersJoined.newBuilder()
        .addAllWorkerInfo(workers)
        .setNumberOfWorkers(workers.size())
        .build();
    JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
        .setAllJoined(allWorkersJoined)
        .build();
    try {
      ZKEventsManager.publishEvent(client, rootPath, jobID, jobEvent);
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  public void publishJobMasterRestarted() {
    // generate en event and inform all other workers
    JobMasterAPI.JobMasterRestarted jmRestarted = JobMasterAPI.JobMasterRestarted.newBuilder()
        .setNumberOfWorkers(numberOfWorkers)
        .setJmAddress(jmAddress)
        .build();

    JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
        .setJmRestarted(jmRestarted)
        .build();
    try {
      ZKEventsManager.publishEvent(client, rootPath, jobID, jobEvent);
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  /**
   * when a worker znode is removed from the ephemeral znode of this job znode,
   * take necessary actions
   * Possibilities:
   * that worker may have completed and deleted its znode,
   * that worker may have failed
   * that worker may have been removed by scaling down
   * a failed and restarted worker may have deleted the znode from its previous run
   */
  private void workerZnodeRemoved(PathChildrenCacheEvent event) {

    // if job master znode removed, it must have failed
    // job master is the last one to leave the job.
    // it does not send complete message as workers when it finishes.
    String workerPath = event.getData().getPath();
    int removedWorkerID = ZKUtils.getWorkerIDFromEphemPath(workerPath);

    // this is a scaled down worker, nothing to do
    if (scaledDownWorkers.contains(removedWorkerID)) {
      scaledDownWorkers.remove(Integer.valueOf(removedWorkerID));
      LOG.info("Removed scaled down worker: " + removedWorkerID);
      return;
    }

    // get worker info and the state from persistent storage
    WorkerWithState workerWithState;
    try {
      workerWithState =
          ZKPersStateManager.getWorkerWithState(client, rootPath, jobID, removedWorkerID);
      if (workerWithState == null) {
        LOG.severe("worker[" + removedWorkerID + "] removed, but its data can not be retrieved.");
        return;
      }
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, "worker[" + removedWorkerID
          + "] removed, but its data can not be retrieved.", e);
      return;
    }

    String workerBodyText = ZKEphemStateManager.decodeWorkerZnodeBody(event.getData().getData());

    // need to distinguish between completed, scaled down and failed workers
    // if a worker completed before, it has left the job by completion
    // if the workerID of removed worker is higher than the number of workers in the job,
    // it means that is a scaled down worker.
    // otherwise, the worker failed. We inform the failureListener.

    if (workerWithState.getState() == JobMasterAPI.WorkerState.COMPLETED) {

      // removed event received for completed worker, nothing to do
      return;

    } else if (ZKEphemStateManager.DELETE_TAG.equals(workerBodyText)) {
      // restarting worker deleted the previous ephemeral znode
      // ignore this event, because the worker is already re-joining
      LOG.info("Restarting worker deleted znode from previous run: " + workerPath);
      return;

    } else {
      // worker failed
      LOG.info(String.format("Worker[%s] FAILED. Worker last status: %s",
          removedWorkerID, workerWithState.getState()));

      workerMonitor.failed(removedWorkerID);

      try {
        ZKPersStateManager.updateWorkerStatus(client, rootPath, jobID,
            workerWithState.getInfo(),
            workerWithState.getRestartCount(),
            JobMasterAPI.WorkerState.FAILED);
      } catch (Twister2Exception e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      }

      publishWorkerFailed(workerWithState.getWorkerID());
    }
  }

  /**
   * when the status of a worker updated in the persistent worker znode,
   * take necessary actions
   */
  private void childZnodeUpdated(PathChildrenCacheEvent event) {
    String childPath = event.getData().getPath();
    int workerID = ZKUtils.getWorkerIDFromPersPath(childPath);
    WorkerWithState workerWithState = WorkerWithState.decode(event.getData().getData());

    LOG.fine(String.format("Worker[%s] status changed to: %s ",
        workerID, workerWithState.getState()));

    // inform workerMonitor when the worker becomes COMPLETED
    if (workerWithState.getState() == JobMasterAPI.WorkerState.COMPLETED) {
      workerMonitor.completed(workerID);
    }
  }

  /**
   * create the listener for barrier event znodes in the barrier directory
   * to determine whether all workers arrived on the barrier
   */
  private void addBarrierChildrenCacheListener(
      PathChildrenCache cache,
      Set<Integer> workersAtBarrier,
      Set<Integer> toBeRemovedWorkers,
      JobMasterAPI.AllArrivedOnBarrier.BarrierType barrierType) {

    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        String childPath = event.getData().getPath();
        int workerID = ZKUtils.getWorkerIDFromPersPath(childPath);

        switch (event.getType()) {
          case CHILD_ADDED:
            workersAtBarrier.add(workerID);
            if (workersAtBarrier.size() == numberOfWorkers
                && toBeRemovedWorkers.isEmpty()) {

              allArrivedAtBarrier(barrierType);
              toBeRemovedWorkers.addAll(workersAtBarrier);
            }
            break;

          case CHILD_REMOVED:
            workersAtBarrier.remove(workerID);
            toBeRemovedWorkers.remove(workerID);
            break;

          default:
            // nothing to do
        }
      }
    };
    cache.getListenable().addListener(listener);
  }

  /**
   * publish all arrived event and let the listener know in the case of init barrier
   */
  private void allArrivedAtBarrier(JobMasterAPI.AllArrivedOnBarrier.BarrierType barrierType) {

    // first inform barrier listener if there is any
    if (barrierListener != null
        && barrierType == JobMasterAPI.AllArrivedOnBarrier.BarrierType.INIT) {

      barrierListener.allArrived();
    }

    // let all workers know
    JobMasterAPI.AllArrivedOnBarrier allArrived = JobMasterAPI.AllArrivedOnBarrier.newBuilder()
        .setNumberOfWorkers(numberOfWorkers)
        .setBarrierType(barrierType)
        .build();

    JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
        .setAllArrived(allArrived)
        .build();
    try {
      ZKEventsManager.publishEvent(client, rootPath, jobID, jobEvent);
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  /**
   * close all local entities.
   */
  public void close() {
    CloseableUtils.closeQuietly(ephemChildrenCache);
    CloseableUtils.closeQuietly(persChildrenCache);
    CloseableUtils.closeQuietly(defaultBarrierCache);
    CloseableUtils.closeQuietly(initBarrierCache);

    if (masterEphemZNode != null) {
      CloseableUtils.closeQuietly(masterEphemZNode);
    }
  }

}
