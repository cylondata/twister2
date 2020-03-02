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
package edu.iu.dsc.tws.master.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.common.zk.JobZNodeManager;
import edu.iu.dsc.tws.common.zk.ZKBarrierManager;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class ZKJobUpdater {
  private static final Logger LOG = Logger.getLogger(ZKJobUpdater.class.getName());

  private Config config;
  private String jobID;

  public ZKJobUpdater(Config config, String jobID) {
    this.config = config;
    this.jobID = jobID;
  }

  /**
   * update number of workers on the Job on ZooKeeper
   */
  public boolean updateState(JobAPI.JobState newState) {

    // if ZooKeeper server is not used, return. Nothing to be done.
    if (!ZKContext.isZooKeeperServerUsed(config)) {
      return true;
    }

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));
    String rootPath = ZKContext.rootNode(config);
    try {
      JobZNodeManager.updateJobState(client, rootPath, jobID, newState);
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not update job znode", e);
      return false;
    }
  }

  /**
   * update number of workers on the Job on ZooKeeper
   */
  public boolean updateWorkers(int workerChange) {

    // if ZooKeeper server is not used, return. Nothing to be done.
    if (!ZKContext.isZooKeeperServerUsed(config)) {
      return true;
    }

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));
    String rootPath = ZKContext.rootNode(config);
    try {
      JobZNodeManager.updateJobWorkers(client, rootPath, jobID, workerChange);
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not update job znode", e);
      return false;
    }
  }

  /**
   * remove InitialState worker znodes after scaling down
   * @return
   */
  public boolean removeInitialStateZNodes(int minWorkerID, int maxWorkerID) {

    // if ZooKeeper server is not used, return. Nothing to be done.
    if (!ZKContext.isZooKeeperServerUsed(config)) {
      return true;
    }

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));
    String rootPath = ZKContext.rootNode(config);
    try {
      ZKPersStateManager.removeScaledDownZNodes(
          client, rootPath, jobID, minWorkerID, maxWorkerID);
      ZKBarrierManager.removeScaledDownZNodes(client, rootPath, jobID, minWorkerID, maxWorkerID);

      return true;
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return false;
    }
  }

}
