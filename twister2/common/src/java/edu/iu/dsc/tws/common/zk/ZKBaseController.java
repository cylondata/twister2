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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.resource.ControllerContext;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IJobMasterListener;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.IWorkerStatusListener;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.JobMasterState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class ZKBaseController {

  public static final Logger LOG = Logger.getLogger(ZKBaseController.class.getName());

  // number of workers in this job
  protected int numberOfWorkers;

  // name of this job
  protected String jobName;

  // the client to connect to ZK server
  protected CuratorFramework client;

  // the path, znode and children cache objects for this job
  protected String jobPath;

  // children cache for job znode
  protected NodeCache jobZnodeCache;

  // children cache for job znode
  protected PathChildrenCache childrenCache;

  // config object
  protected Config config;
  protected String rootPath;

  // a flag to show whether all workers joined the job
  // Initially it is false. It becomes true when all workers joined the job.
  protected boolean allJoined = false;

  // synchronization object for waiting all workers to join the job
  protected Object waitObject = new Object();

  // all workers in the job, including completed and failed ones
  protected HashMap<WorkerInfo, WorkerState> jobWorkers;

  // Inform worker failure events
  protected IWorkerFailureListener failureListener;

  // Inform worker events
  protected IWorkerStatusListener workerStatusListener;

  // Inform when all workers joined the job
  protected IAllJoinedListener allJoinedListener;

  // Inform events related to job master
  protected IJobMasterListener jobMasterListener;

  // job master related data
  protected String jmAddress;
  protected JobMasterState jmState;

  // list of scaled down workers
  // when the job scaled down, we populate this list
  // we remove each ID when we received worker znode removed event
  private List<Integer> scaledDownWorkers = new LinkedList<>();

  protected ZKBaseController(Config config,
                             String jobName,
                             int numberOfWorkers) {
    this.config = config;
    this.rootPath = ZKContext.rootNode(config);

    this.jobName = jobName;
    this.numberOfWorkers = numberOfWorkers;
    this.jobPath = ZKUtils.constructWorkersEphemDir(rootPath, jobName);

    jobWorkers = new HashMap<>(numberOfWorkers);
  }

  /**
   * connect to the server
   * append this worker info to the body of job znode
   * create an ephemeral znode for this worker
   * worker znode body has the state of this worker
   * it will be updated as the status of worker changes from STARTING, RUNNING, COMPLETED
   */
  protected void initialize() throws Exception {

    try {
      String zkServerAddresses = ZKContext.serverAddresses(config);
      int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(config);
      client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);

      // update numberOfWorkers from jobZnode
      // with scaling up/down, it may have been changed
      JobAPI.Job job = ZKJobZnodeUtil.readJobZNodeBody(client, jobName, config);
      numberOfWorkers = job.getNumberOfWorkers();

      // cache jobZnode data, and watch that node for changes
      jobZnodeCache = new NodeCache(client, jobPath);
      addJobNodeListener();
      jobZnodeCache.start();

      // We cache children data for parent path.
      // So we will listen for all workers in the job
      childrenCache = new PathChildrenCache(client, jobPath, true);
      addChildrenCacheListener(childrenCache);
      childrenCache.start();

    } catch (Exception e) {
      throw e;
    }
  }

  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  public WorkerInfo getWorkerInfoForID(int id) {
    return jobWorkers
        .keySet()
        .stream()
        .filter(wInfo -> wInfo.getWorkerID() == id)
        .findFirst()
        .orElse(null);
  }

  public WorkerState getWorkerStatusForID(int id) {
    return jobWorkers
        .entrySet()
        .stream()
        .filter(entry -> entry.getKey().getWorkerID() == id)
        .findFirst()
        .map(entry -> entry.getValue())
        .orElse(null);
  }

  /**
   * Get all joined workers including the ones that have already completed or failed
   */
  public List<WorkerInfo> getJoinedWorkers() {

    return cloneJobWorkers();
  }

  /**
   * count joined and live workers
   */
  private int countJoinedWorkers() {

    int count = 0;
    for (WorkerState state : jobWorkers.values()) {
      if (state == WorkerState.STARTED
          || state == WorkerState.RESTARTED
          || state == WorkerState.COMPLETED) {
        count++;
      }
    }
    return count;
  }

  /**
   * create a mirror of jobWorkers
   * do not create clones of each WorkerInfo, since they are read only
   */
  protected List<WorkerInfo> cloneJobWorkers() {

    List<WorkerInfo> clone = new LinkedList<>();
    for (WorkerInfo info : jobWorkers.keySet()) {
      clone.add(info);
    }
    return clone;
  }

  /**
   * remove the workerInfo for the given ID if it exists
   */
  protected void removeWorkerInfo(int workerID) {
    jobWorkers.keySet().removeIf(wInfo -> wInfo.getWorkerID() == workerID);
  }


  /**
   * add a single IWorkerStatusListener
   * if an additional IWorkerStatusListener tried to be added,
   * do not add and return false
   */
  public boolean addWorkerStatusListener(IWorkerStatusListener iWorkerStatusListener) {
    if (this.workerStatusListener != null) {
      return false;
    }

    this.workerStatusListener = iWorkerStatusListener;
    return true;
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
    return true;
  }

  /**
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
   * wait to make sure that the number of workers reached the total number of workers in the job
   * return all joined workers in the job including the ones that have already left or failed
   * throws an exception if the timeLimit is reached
   */
  public List<WorkerInfo> getAllWorkers() throws TimeoutException {

    // if all workers already joined, return the workers list
    if (allJoined) {
      return cloneJobWorkers();
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
            return cloneJobWorkers();
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
      return cloneJobWorkers();
    } else {
      throw new TimeoutException("Not all workers joined the job on the given time limit: "
          + timeLimit + "ms.");
    }
  }

  private void addJobNodeListener() {
    NodeCacheListener listener = new NodeCacheListener() {
      @Override
      public void nodeChanged() throws Exception {
        byte[] jobZnodeBodyBytes = client.getData().forPath(jobPath);
        JobAPI.Job job = ZKJobZnodeUtil.decodeJobZnode(jobZnodeBodyBytes);
        LOG.info("Job znode changed. New number of workers: " + job.getNumberOfWorkers());

        if (numberOfWorkers == job.getNumberOfWorkers()) {
          // if numberOfWorkers in this worker and the one in the job are equal,
          // nothing to be done
          // the job is not scaled up or down

        } else if (numberOfWorkers < job.getNumberOfWorkers()) {
          // this means the job is scaled up
          // update numberOfWorkers
          // unset the flag allJoined
          numberOfWorkers = job.getNumberOfWorkers();
          allJoined = false;
        } else {
          // the job is scaled down
          // add removed workers to scaled down list
          for (int i = job.getNumberOfWorkers(); i < numberOfWorkers; i++) {
            scaledDownWorkers.add(i);
          }
          LOG.info("Scaled down workers: " + scaledDownWorkers);
          // update numberOfWorkers
          numberOfWorkers = job.getNumberOfWorkers();
          // remove scaled down workers from worker list
          jobWorkers.keySet().removeIf(wInfo -> wInfo.getWorkerID() >= numberOfWorkers);
        }
      }
    };

    jobZnodeCache.getListenable().addListener(listener);
  }


  private void addChildrenCacheListener(PathChildrenCache cache) {
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        switch (event.getType()) {
          case CHILD_ADDED:
            childZnodeAdded(event);
            break;

          case CHILD_UPDATED:
            childZnodeUpdated(event);
            break;

          case CHILD_REMOVED:
            childZnodeRemoved(event);
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
  private void childZnodeAdded(PathChildrenCacheEvent event) {

    // first determine whether the job master has joined
    // job master path ends with "jm".
    // worker paths end with workerID
    String addedChildPath = event.getData().getPath();
    if (addedChildPath.endsWith("jm")) {
      jobMasterZNodeAdded(event);
      return;
    }

    // if a worker joined
    Pair<WorkerInfo, WorkerState> pair = ZKUtils.decodeWorkerZnode(event.getData().getData());
    int newWorkerID = pair.getKey().getWorkerID();

    // if the workerID of joined worker is higher than numberOfWorkers in the job,
    // ignore this event with a warning log message.
    if (newWorkerID >= numberOfWorkers) {
      LOG.severe(String.format(
          "A worker joined but its workerID[%s] is higher than numberOfWorkers[%s]. "
              + "Ignoring this join event. WorkerInfo of ignored worker: %s",
          newWorkerID, numberOfWorkers, pair.getKey()));
      return;
    }

    // if the status of joining worker is RESTARTING, it is coming from failure
    // if there is already an entry in worker map, remove it. Then add new one.
    // inform the listener if any
    if (pair.getValue() == WorkerState.RESTARTED) {
      removeWorkerInfo(newWorkerID);
      jobWorkers.put(pair.getKey(), pair.getValue());

      if (failureListener != null) {
        failureListener.restarted(pair.getKey());
      }

      LOG.info("A worker rejoined the job with ID: " + newWorkerID);

      // if the status of joining worker is not RESTARTING,
      // it can be STARTING, or already RUNNING, or COMPLETED,
      // add it to the list
    } else {
      // there should not be any existing workerInfo, if any, overwrite it
      WorkerInfo existingWorkerInfo = getWorkerInfoForID(newWorkerID);
      if (existingWorkerInfo != null) {
        LOG.warning("A worker joined but there is already a worker with that id: " + newWorkerID
            + ". Overwriting existing workerInfo with this one: " + pair.getKey());
        removeWorkerInfo(newWorkerID);
      } else {
        LOG.info("A new worker joined the job with ID: " + newWorkerID);
      }

      jobWorkers.put(pair.getKey(), pair.getValue());

      // inform worker status listener
      if (workerStatusListener != null) {
        workerStatusListener.started(pair.getKey());
      }
    }

    // if currently all workers exist in the job, let the workers know that all joined
    // we don't check the size of jobWorkers,
    // because some workers may have joined and failed.
    // This shows currently existing workers in the job group
    if (numberOfWorkers == countJoinedWorkers() && !allJoined) {
      allJoined = true;
      synchronized (waitObject) {
        waitObject.notify();
      }

      // inform the allJoinedListener
      if (allJoinedListener != null) {
        allJoinedListener.allWorkersJoined(getJoinedWorkers());
      }
    }
  }

  /**
   * take necessary actions when a job master znode added
   */
  private void jobMasterZNodeAdded(PathChildrenCacheEvent event) {
    Pair<String, JobMasterState> pair = ZKUtils.decodeJobMasterZnode(event.getData().getData());

    // If the status of joining job master is JM_STARTED. It is joining the job for the first time.
    if (pair.getValue() == JobMasterState.JM_STARTED) {
      jmAddress = pair.getKey();
      jmState = pair.getValue();
      LOG.info("Job Master joined the job: " + jmAddress + ", " + jmState);

      // inform job master listener
      if (jobMasterListener != null) {
        jobMasterListener.jobMasterJoined(jmAddress);
      }

      return;
    }

    // If the status of joining job master is JM_RESTARTED. It is coming from failure.
    if (pair.getValue() == JobMasterState.JM_RESTARTED) {
      jmAddress = pair.getKey();
      jmState = pair.getValue();
      LOG.info("Job Master rejoined after failure: " + jmAddress + ", " + jmState);

      // inform job master listener
      if (jobMasterListener != null) {
        jobMasterListener.jobMasterRejoined(jmAddress);
      }
      return;
    }

    // if the status of joining job master is something else,
    // log a warning message and ignore this event
    LOG.warning("Job Master joined but its initial state is neither STARTING nor RESTARTING. "
        + " Something may be wrong. Ignoring this join event. JM address and its status: "
        + pair.getKey() + ", " + pair.getValue());

    return;
  }

  /**
   * when a child znode content is updated,
   * take necessary actions
   */
  private void childZnodeUpdated(PathChildrenCacheEvent event) {
    String addedChildPath = event.getData().getPath();
    if (addedChildPath.endsWith("jm")) {
      Pair<String, JobMasterState> pair = ZKUtils.decodeJobMasterZnode(event.getData().getData());
      jmState = pair.getValue();

      LOG.info("Job Master status changed to: " + jmState);

      return;
    }

    Pair<WorkerInfo, WorkerState> pair = ZKUtils.decodeWorkerZnode(event.getData().getData());

    // update the worker state in the map
    jobWorkers.put(pair.getKey(), pair.getValue());

    LOG.info(String.format("Worker[%s] status changed to: %s ",
        pair.getKey().getWorkerID(), pair.getValue()));

    // inform the listener if the worker becomes COMPLETED
    if (workerStatusListener != null && pair.getValue() == WorkerState.COMPLETED) {
      workerStatusListener.completed(pair.getKey().getWorkerID());
    }
  }

  /**
   * when a znode is removed from this job znode,
   * take necessary actions
   */
  private void childZnodeRemoved(PathChildrenCacheEvent event) {

    // if job master znode removed, it must have failed
    // job master is the last one to leave the job.
    // it does not send complete message as workers when it finishes.
    String childPath = event.getData().getPath();
    if (childPath.endsWith("jm")) {

      jmState = JobMasterState.JM_FAILED;
      LOG.warning("Job Master failed. ----------------------------- ");

      // inform the job master listener
      if (jobMasterListener != null) {
        jobMasterListener.jobMasterFailed();
      }

      return;
    }

    Pair<WorkerInfo, WorkerState> pair = ZKUtils.decodeWorkerZnode(event.getData().getData());

    // need to distinguish between completed, scaled down and failed workers
    // if a worker completed before, it has left the job by completion
    // if the workerID of removed worker is higher than the number of workers in the job,
    // it means that is a scaled down worker.
    // otherwise, the worker failed. We inform the failureListener.
    int removedWorkerID = pair.getKey().getWorkerID();

    // this is the scaled down worker
    if (scaledDownWorkers.contains(removedWorkerID)) {

      scaledDownWorkers.remove(Integer.valueOf(removedWorkerID));
      LOG.info("Removed scaled down worker: " + removedWorkerID);

    } else if (pair.getValue() == WorkerState.COMPLETED) {

      // removed event received for completed worker, nothing to do

    } else {
      // worker failed
      LOG.info(String.format("Worker[%s] FAILED. Worker last status: %s",
          removedWorkerID, pair.getValue()));

      if (failureListener != null) {
        // first change worker state into failed in local list
        WorkerInfo failedWorker = getWorkerInfoForID(removedWorkerID);
        jobWorkers.put(failedWorker, WorkerState.FAILED);
        // inform the listener
        failureListener.failed(failedWorker.getWorkerID());
      }
    }
  }

  /**
   * close all local entities.
   */
  public void close() {
    try {
      jobZnodeCache.close();
      childrenCache.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when closing", e);
    }
  }

}
