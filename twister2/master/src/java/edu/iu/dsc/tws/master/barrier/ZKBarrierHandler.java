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
package edu.iu.dsc.tws.master.barrier;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.primitives.Longs;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKEventsManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class ZKBarrierHandler implements BarrierResponder {
  private static final Logger LOG = Logger.getLogger(ZKBarrierHandler.class.getName());

  private BarrierMonitor barrierMonitor;
  private Config config;
  private String jobID;

  // the client to connect to ZK server
  private CuratorFramework client;

  // children cache for default barrier directory for determining barrier arrival of all workers
  private PathChildrenCache defaultBarrierCache;

  // children cache for default barrier directory for determining barrier arrival of all workers
  private PathChildrenCache initBarrierCache;

  private String rootPath;

  private TreeSet<Integer> initialWorkersAtDefault = new TreeSet<>();
  private TreeSet<Integer> initialWorkersAtInit = new TreeSet<>();

  public ZKBarrierHandler(BarrierMonitor barrierMonitor, Config config, String jobID) {
    this.barrierMonitor = barrierMonitor;
    this.config = config;
    this.jobID = jobID;

    rootPath = ZKContext.rootNode(config);
  }

  /**
   * initialize ZKBarrierHandler,
   * create znode children caches for job master to watch barrier events
   */
  public void initialize(JobMasterAPI.JobMasterState initialState) throws Twister2Exception {

    if (!(initialState == JobMasterAPI.JobMasterState.JM_STARTED
        || initialState == JobMasterAPI.JobMasterState.JM_RESTARTED)) {
      throw new Twister2Exception("initialState has to be either JobMasterState.JM_STARTED or "
          + "JobMasterState.JM_RESTARTED. Supplied value: " + initialState);
    }

    try {
      String zkServerAddresses = ZKContext.serverAddresses(config);
      int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(config);
      client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);

      // update numberOfWorkers from jobZnode
      // with scaling up/down, it may have been changed
      if (initialState == JobMasterAPI.JobMasterState.JM_RESTARTED) {

        // do not get previous events on barriers
        // get current snapshots of both barriers at the restart
        String defaultBarrierDir = ZKUtils.defaultBarrierDir(rootPath, jobID);
        defaultBarrierCache = new PathChildrenCache(client, defaultBarrierDir, true);
        addBarrierChildrenCacheListener(defaultBarrierCache, JobMasterAPI.BarrierType.DEFAULT);
        defaultBarrierCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        addInitialWorkersAtBarrier(defaultBarrierCache, initialWorkersAtDefault);
        LOG.info("Existing workers at default barrier: " + initialWorkersAtDefault.size());

        // do not get previous events on barriers
        // get current snapshots of both barriers at restart
        String initBarrierDir = ZKUtils.initBarrierDir(rootPath, jobID);
        initBarrierCache = new PathChildrenCache(client, initBarrierDir, true);
        addBarrierChildrenCacheListener(initBarrierCache, JobMasterAPI.BarrierType.INIT);
        initBarrierCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        addInitialWorkersAtBarrier(initBarrierCache, initialWorkersAtInit);
        LOG.info("Existing workers at init barrier: " + initialWorkersAtInit.size());

      } else {

        // We listen for status updates for the default barrier path
        String defaultBarrierDir = ZKUtils.defaultBarrierDir(rootPath, jobID);
        defaultBarrierCache = new PathChildrenCache(client, defaultBarrierDir, true);
        addBarrierChildrenCacheListener(defaultBarrierCache, JobMasterAPI.BarrierType.DEFAULT);
        defaultBarrierCache.start();

        // We listen for status updates for the init barrier path
        String initBarrierDir = ZKUtils.initBarrierDir(rootPath, jobID);
        initBarrierCache = new PathChildrenCache(client, initBarrierDir, true);
        addBarrierChildrenCacheListener(initBarrierCache, JobMasterAPI.BarrierType.INIT);
        initBarrierCache.start();
      }

    } catch (Twister2Exception e) {
      throw e;
    } catch (Exception e) {
      throw new Twister2Exception("Exception when initializing ZKMasterController.", e);
    }
  }

  public TreeSet<Integer> getInitialWorkersAtDefault() {
    return initialWorkersAtDefault;
  }

  public TreeSet<Integer> getInitialWorkersAtInit() {
    return initialWorkersAtInit;
  }

  private void addInitialWorkersAtBarrier(PathChildrenCache childrenCache,
                                          Set<Integer> workersAtBarrier) {

    List<ChildData> existingWorkerZnodes = childrenCache.getCurrentData();
    for (ChildData child: existingWorkerZnodes) {
      String fullPath = child.getPath();
      int workerID = ZKUtils.getWorkerIDFromPersPath(fullPath);
      workersAtBarrier.add(workerID);
    }
  }

  /**
   * create the listener for barrier event znodes in the barrier directory
   * to determine whether all workers arrived on the barrier
   */
  private void addBarrierChildrenCacheListener(
      PathChildrenCache cache,
      JobMasterAPI.BarrierType barrierType) {

    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        String childPath = event.getData().getPath();
        int workerID = ZKUtils.getWorkerIDFromPersPath(childPath);
        long timeout = Longs.fromByteArray(event.getData().getData());

        switch (event.getType()) {
          case CHILD_ADDED:
            if (barrierType == JobMasterAPI.BarrierType.DEFAULT) {
              barrierMonitor.arrivedAtDefault(workerID, timeout);
            } else if (barrierType == JobMasterAPI.BarrierType.INIT) {
              barrierMonitor.arrivedAtInit(workerID, timeout);
            }
            break;

          case CHILD_REMOVED:
            if (barrierType == JobMasterAPI.BarrierType.DEFAULT) {
              barrierMonitor.removedFromDefault(workerID);
            } else if (barrierType == JobMasterAPI.BarrierType.INIT) {
              barrierMonitor.removedFromInit(workerID);
            }
            break;

          default:
            // nothing to do
        }
      }
    };
    cache.getListenable().addListener(listener);
  }

  /**
   * publish all arrived event
   */
  @Override
  public void allArrived(JobMasterAPI.BarrierType barrierType) {
    publishBarrierDone(barrierType, JobMasterAPI.BarrierResult.SUCCESS);
  }

  @Override
  public void barrierFailed(JobMasterAPI.BarrierType barrierType,
                            JobMasterAPI.BarrierResult result) {
    publishBarrierDone(barrierType, result);
  }

  /**
   * publish BarrierDone event
   */
  public void publishBarrierDone(JobMasterAPI.BarrierType barrierType,
                                 JobMasterAPI.BarrierResult barrierResult) {
    // let all workers know
    JobMasterAPI.BarrierDone barrierDone = JobMasterAPI.BarrierDone.newBuilder()
        .setBarrierType(barrierType)
        .setResult(barrierResult)
        .build();

    JobMasterAPI.JobEvent jobEvent = JobMasterAPI.JobEvent.newBuilder()
        .setBarrierDone(barrierDone)
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
    CloseableUtils.closeQuietly(defaultBarrierCache);
    CloseableUtils.closeQuietly(initBarrierCache);
  }
}
