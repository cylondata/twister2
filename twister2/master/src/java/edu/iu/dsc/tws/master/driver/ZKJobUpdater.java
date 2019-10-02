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

import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKJobZnodeUtil;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class ZKJobUpdater {

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
    String jobPath = ZKUtils.constructJobPath(ZKContext.rootNode(config), job.getJobName());
    try {
      ZKJobZnodeUtil.updateJobZNode(client, job, jobPath);
    } catch (Exception e) {
      throw new Twister2Exception("Could not update job znode", e);
    } finally {
      ZKUtils.closeClient();
    }

  }
}
