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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.common.zk.WorkerWithState;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKEphemStateManager;
import edu.iu.dsc.tws.common.zk.ZKEventsManager;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.JobMasterState;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class ZKMasterController {
  public static final Logger LOG = Logger.getLogger(ZKMasterController.class.getName());

  // number of workers in this job
  protected int numberOfWorkers;
  protected String jobName;

  // config object
  protected Config config;
  protected String rootPath;
  protected String persDir;
  protected String ephemDir;
  protected String barrierDir;

  // Job Master IP address
  private String masterAddress;

  // the client to connect to ZK server
  protected CuratorFramework client;

  // children cache for persistent job znode
  protected PathChildrenCache ephemChildrenCache;

  // children cache for persistent job znode
  protected PathChildrenCache persChildrenCache;

  // children cache for barrier directory
  protected PathChildrenCache barrierChildrenCache;

  // persistent ephemeral znode for this worker
  private PersistentNode masterEphemZNode;

  // list of scaled down workers
  // when the job scaled down, we populate this list
  // we remove each ID when we received worker znode removed event
  private List<Integer> scaledDownWorkers = new LinkedList<>();

  private WorkerMonitor workerMonitor;

  private int workerCountAtBarrier = 0;

  public ZKMasterController(Config config,
                            String jobName,
                            int numberOfWorkers,
                            String masterAddress,
                            WorkerMonitor workerMonitor) {
    this.config = config;
    this.jobName = jobName;
    this.numberOfWorkers = numberOfWorkers;
    this.masterAddress = masterAddress;
    this.workerMonitor = workerMonitor;

    rootPath = ZKContext.rootNode(config);
    persDir = ZKUtils.persDir(rootPath, jobName);
    ephemDir = ZKUtils.ephemDir(rootPath, jobName);
    barrierDir = ZKUtils.barrierDir(rootPath, jobName);
  }

  public void jobScaledUp(int newNumberOfWorkers) {
    this.numberOfWorkers = newNumberOfWorkers;
  }

  public void jobScaledDown(int newNumberOfWorkers) {
    scaledDownWorkers = new LinkedList<>();
    for (int i = newNumberOfWorkers; i < numberOfWorkers; i++) {
      scaledDownWorkers.add(i);
    }

    this.numberOfWorkers = newNumberOfWorkers;
  }

  /**
   * create an ephemeral znode for the job master
   * set the master address in the body of that node
   * job master status also goes into the body of that znode
   * The body of the worker znode will be updated as the status of the job master changes
   * from STARTING, RUNNING, COMPLETED
   */
  public void initialize(JobMasterState initialState) throws Exception {

    if (!(initialState == JobMasterState.JM_STARTED
        || initialState == JobMasterState.JM_RESTARTED)) {
      throw new Exception("initialState has to be either WorkerState.STARTED or "
          + "WorkerState.RESTARTED. Supplied value: " + initialState);
    }

    try {
      String zkServerAddresses = ZKContext.serverAddresses(config);
      int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(config);
      client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);

      // update numberOfWorkers from jobZnode
      // with scaling up/down, it may have been changed
      if (initialState == JobMasterState.JM_RESTARTED) {
        JobAPI.Job job = ZKPersStateManager.readJobZNode(client, rootPath, jobName);
        numberOfWorkers = job.getNumberOfWorkers();
      }

      // We listen for join/remove events for ephemeral children
      ephemChildrenCache = new PathChildrenCache(client, ephemDir, true);
      addEphemChildrenCacheListener(ephemChildrenCache);
      ephemChildrenCache.start();

      // We listen for status updates for persistent path
      persChildrenCache = new PathChildrenCache(client, persDir, true);
      addPersChildrenCacheListener(persChildrenCache);
      persChildrenCache.start();

      // We listen for status updates for persistent path
      barrierChildrenCache = new PathChildrenCache(client, barrierDir, true);
      addBarrierChildrenCacheListener(barrierChildrenCache);
      barrierChildrenCache.start();

      // TODO: we nay need to create ephemeral job master znode so that
      //   workers can know when jm fails
      //   createJobMasterZnode(initialState);

      LOG.info("Job Master: " + masterAddress + " initialized successfully.");

    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * create ephemeral znode for job master
   */
  private void createJMEphemZnode(JobMasterState initialState) {
    String jmPath = ZKUtils.jmEphemPath(rootPath, jobName);

    // put masterAddress and its state into znode body
    byte[] jmZnodeBody = ZKUtils.encodeJobMasterZnode(masterAddress, initialState.getNumber());
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

  private void addBarrierChildrenCacheListener(PathChildrenCache cache) {
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        switch (event.getType()) {
          case CHILD_ADDED:
            barrierZnodeAdded(event);
            break;

          case CHILD_REMOVED:
            workerCountAtBarrier--;
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
  private void workerZnodeAdded(PathChildrenCacheEvent event) {

    boolean initialAllJoined = workerMonitor.isAllJoined();

    String addedChildPath = event.getData().getPath();
    int workerID = ZKUtils.getWorkerIDFromEphemPath(addedChildPath);
    WorkerWithState workerWithState =
        ZKPersStateManager.getWorkerWithState(client, persDir, workerID);

    // if the status of joining worker is RESTARTED, it is coming from failure
    if (workerWithState.getState() == JobMasterAPI.WorkerState.RESTARTED) {

      workerMonitor.restarted(workerWithState);

      JobMasterAPI.WorkerRestarted workerRestarted = JobMasterAPI.WorkerRestarted.newBuilder()
              .setWorkerInfo(workerWithState.getInfo())
              .build();

      JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
          .setRestarted(workerRestarted)
          .build();
      ZKEventsManager.publishEvent(client, rootPath, jobName, jobEvent);

    } else if (workerWithState.getState() == JobMasterAPI.WorkerState.STARTED) {

      workerMonitor.started(workerWithState);

      // a worker joined with initial state that is not acceptable
    } else {
      LOG.warning("Following worker joined with initial state of " + workerWithState.getState()
          + "Something must be wrong. Ignoring this event. WorkerInfo: "
          + workerWithState.getInfo());
      return;
    }

    // if currently all workers exist in the job, let the workers know that all joined
    if (!initialAllJoined && workerMonitor.isAllJoined()) {
      publishAllJoined();
    }
  }

  public void publishAllJoined() {
    List<JobMasterAPI.WorkerInfo> workers = workerMonitor.getWorkerInfoList();

    JobMasterAPI.AllWorkersJoined allWorkersJoined = JobMasterAPI.AllWorkersJoined.newBuilder()
        .addAllWorkerInfo(workers)
        .build();
    JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
        .setAllJoined(allWorkersJoined)
        .build();
    ZKEventsManager.publishEvent(client, rootPath, jobName, jobEvent);
  }

  /**
   * when a znode is removed from this job znode,
   * take necessary actions
   */
  private void workerZnodeRemoved(PathChildrenCacheEvent event) {

    // if job master znode removed, it must have failed
    // job master is the last one to leave the job.
    // it does not send complete message as workers when it finishes.
    String workerPath = event.getData().getPath();
    int removedWorkerID = ZKUtils.getWorkerIDFromEphemPath(workerPath);
    WorkerWithState workerWithState =
        ZKPersStateManager.getWorkerWithState(client, persDir, removedWorkerID);

    String workerBodyText = ZKEphemStateManager.decodeWorkerZnodeBody(event.getData().getData());

    // need to distinguish between completed, scaled down and failed workers
    // if a worker completed before, it has left the job by completion
    // if the workerID of removed worker is higher than the number of workers in the job,
    // it means that is a scaled down worker.
    // otherwise, the worker failed. We inform the failureListener.

    // this is the scaled down worker
    if (scaledDownWorkers.contains(removedWorkerID)) {

      scaledDownWorkers.remove(Integer.valueOf(removedWorkerID));
      LOG.info("Removed scaled down worker: " + removedWorkerID);
      return;

    } else if (workerWithState.getState() == JobMasterAPI.WorkerState.COMPLETED) {

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

      ZKPersStateManager.updateWorkerStatus(
          client, rootPath, jobName, workerWithState.getInfo(), JobMasterAPI.WorkerState.FAILED);

      JobMasterAPI.WorkerFailed workerFailed = JobMasterAPI.WorkerFailed.newBuilder()
          .setWorkerID(workerWithState.getWorkerID())
          .build();

      JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
          .setFailed(workerFailed)
          .build();
      ZKEventsManager.publishEvent(client, rootPath, jobName, jobEvent);
    }
  }

  /**
   * when a child znode content is updated,
   * take necessary actions
   */
  private void childZnodeUpdated(PathChildrenCacheEvent event) {
    String childPath = event.getData().getPath();
    int workerID = ZKUtils.getWorkerIDFromPersPath(childPath);
    WorkerWithState workerWithState =
        ZKPersStateManager.getWorkerWithState(client, persDir, workerID);

    // TODO: make fine
    LOG.info(String.format("Worker[%s] status changed to: %s ",
        workerID, workerWithState.getState()));

    // inform workerMonitor when the worker becomes COMPLETED
    if (workerWithState.getState() == JobMasterAPI.WorkerState.COMPLETED) {
      workerMonitor.completed(workerID);
    }
  }

  private void barrierZnodeAdded(PathChildrenCacheEvent event) {
    workerCountAtBarrier++;

    if (workerCountAtBarrier == numberOfWorkers) {
      JobMasterAPI.AllArrivedOnBarrier allArrived = JobMasterAPI.AllArrivedOnBarrier.newBuilder()
          .build();

      JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
          .setAllArrived(allArrived)
          .build();
      ZKEventsManager.publishEvent(client, rootPath, jobName, jobEvent);
    }
  }

  /**
   * close all local entities.
   */
  public void close() {
    CloseableUtils.closeQuietly(ephemChildrenCache);
    CloseableUtils.closeQuietly(persChildrenCache);
    CloseableUtils.closeQuietly(barrierChildrenCache);

    if (masterEphemZNode != null) {
      CloseableUtils.closeQuietly(masterEphemZNode);
    }

//    ZKUtils.closeClient();
  }


}
