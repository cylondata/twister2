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
package edu.iu.dsc.tws.rsched.zk;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class ZKJobMonitor {
  public static final Logger LOG = Logger.getLogger(ZKJobMonitor.class.getName());

  // name of this job
  private String jobName;

  private String jobPath;

  private Config config;

  // the client to connect to ZK server
  private CuratorFramework client;

  // children cache for job znode
  private PathChildrenCache workerChildrenCache;

  public ZKJobMonitor(Config config, String jobName) {
    this.config = config;
    this.jobName = jobName;
    this.jobPath = ZKUtils.constructJobPath(ZKContext.rootNode(config), jobName);
  }

  public boolean initialize() throws Exception {

    try {
      String zkServerAddresses = ZKContext.serverAddresses(config);
      client = CuratorFrameworkFactory.newClient(zkServerAddresses,
          new ExponentialBackoffRetry(1000, 3));
      client.start();

      return initialize(client);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when connecting to ZooKeeper Server", e);
      throw e;
    }
  }

  public boolean initialize(CuratorFramework clnt) throws Exception {

    this.client = clnt;

    try {
      // We childrenCache children data for parent path.
      // So we will listen for all workers in the job
      workerChildrenCache = new PathChildrenCache(client, jobPath, false);
      workerChildrenCache.start();

      addListener(workerChildrenCache);

      LOG.info("ZKJobsMonitor: initialized successfully.");
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when initializing ZKJobGroup", e);
      throw e;
    }
  }

  private void addListener(PathChildrenCache cache) {
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event) {

        Pair<JobMasterAPI.WorkerInfo, JobMasterAPI.WorkerState> pair;

        switch (event.getType()) {
          case CHILD_ADDED:
            String workerID = ZKPaths.getNodeFromPath(event.getData().getPath());
            try {
              JobAPI.Job job = ZKJobZnodeUtil.readJobZNodeBody(clientOfEvent, jobName, config);
              LOG.info("Job added: " + job);
            } catch (Exception e) {
              LOG.log(Level.SEVERE, e.getMessage(), e);
            }

            break;

          case CHILD_UPDATED:
            jobName = ZKPaths.getNodeFromPath(event.getData().getPath());
            LOG.info("Job updated: " + jobName);
            break;

          // need to distinguish between completed and failed workers
          // need to inform the worker for other worker failures
          case CHILD_REMOVED:
            jobName = ZKPaths.getNodeFromPath(event.getData().getPath());
            LOG.info("Job removed: " + jobName);
            break;

          default:
            // nothing to do
        }
      }
    };
    cache.getListenable().addListener(listener);
  }

}
