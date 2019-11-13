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
package edu.iu.dsc.tws.master.driver;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKJobZnodeUtil;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class ZKJobUpdater {
  private static final Logger LOG = Logger.getLogger(ZKJobUpdater.class.getName());

  private Config config;

  public ZKJobUpdater(Config config) {
    this.config = config;
  }

  /**
   * update Job on ZooKeeper
   * @param job
   * @return
   */
  public void updateJob(JobAPI.Job job) throws Twister2Exception {

    // if ZooKeeper server is not used, return. Nothing to be done.
    if (!ZKContext.isZooKeeperServerUsed(config)) {
      return;
    }

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));
    String jobPath = ZKUtils.constructJobEphemPath(ZKContext.rootNode(config), job.getJobName());
    try {
      ZKJobZnodeUtil.updateJobZNode(client, job, jobPath);
    } catch (Exception e) {
      throw new Twister2Exception("Could not update job znode", e);
    }

  }

  /**
   * remove InitialState worker znodes after scaling down
   * @return
   */
  public boolean removeInitialStateZNodes(String jobName, int minWorkerID, int maxWorkerID) {

    // if ZooKeeper server is not used, return. Nothing to be done.
    if (!ZKContext.isZooKeeperServerUsed(config)) {
      return true;
    }

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));
    String rootPath = ZKContext.rootNode(config);
    try {
      ZKPersStateManager.removeScaledDownZNodes(
          client, rootPath, jobName, minWorkerID, maxWorkerID);
      return true;
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return false;
    }
  }

}
