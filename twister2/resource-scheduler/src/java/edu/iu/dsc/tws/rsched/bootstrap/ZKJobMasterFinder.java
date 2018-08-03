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
package edu.iu.dsc.tws.rsched.bootstrap;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;

import edu.iu.dsc.tws.common.config.Config;

/**
 * this class is used to discover the Job Master ip address and port number through ZooKeeper
 * JobMaster creates a znode at the ZooKeeper server
 * this class queries this node and retrieves the job master address from the content of that znode
 *
 * When that znode can not be retrieved immediately,
 * it creates a NodeCache object and gets the value from this cache,
 * instead of querying continually
 */
public class ZKJobMasterFinder {
  private static final Logger LOG = Logger.getLogger(ZKJobMasterFinder.class.getName());

  private Config config;

  private CuratorFramework client;
  private String jobMasterPath;
  private NodeCache jobMasterNodeCache;
  private String jobMasterIP;
  private String jobMasterPort;

  public ZKJobMasterFinder(Config config) {
    this.config = config;
    jobMasterPath = ZKUtil.constructJobMasterPath(config);
  }

  /**
   * connect to ZooKeeper server
   * @return
   */
  public void initialize() {
    client = ZKUtil.connectToServer(config);
  }

  public String getJobMasterIPandPort() {

    // if the job master address already retrieved, return it
    if (jobMasterIP != null) {
      return jobMasterIP + ":" + jobMasterPort;
    }

    // check whether the job master node is created
    // if not, return null
    try {
      if (client.checkExists().forPath(jobMasterPath) == null) {
        return null;
      }

      byte[] parentData = client.getData().forPath(jobMasterPath);
      String jobMasterIPandPort = new String(parentData);
      setJobMasterIPandPort(jobMasterIPandPort);
      return jobMasterIPandPort;

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when trying to retrieve Job Master adress from ZK", e);
      return null;
    }
  }

  private void setJobMasterIPandPort(String jobMasterIPandPort) {
    jobMasterPort = jobMasterIPandPort.substring(jobMasterIPandPort.lastIndexOf(":") + 1);
    jobMasterIP = jobMasterIPandPort.substring(0, jobMasterIPandPort.lastIndexOf(":"));
  }

  public String waitAndGetJobMasterIPandPort(long timeLimit) {

    // first try to get it from the ZooKeeper server directly
    String jobMasterIPandPort = getJobMasterIPandPort();
    if (jobMasterIPandPort != null) {
      return jobMasterIPandPort;
    }

    long duration = 0;
    while (duration < timeLimit) {
      jobMasterIPandPort = getJobMasterIPandPortFromCache();
      if (jobMasterIPandPort == null) {
        try {
          Thread.sleep(50);
          duration += 50;
        } catch (InterruptedException e) {
          LOG.log(Level.INFO, "Thread sleep interrupted. Will try again ...", e);
        }
      } else {
        return jobMasterIPandPort;
      }
    }

    LOG.severe("Waited for Job Master to join, but timeLimit has been reached");
    return null;
  }

  private String getJobMasterIPandPortFromCache() {

    // if the job master address already retrieved, return it
    if (jobMasterIP != null) {
      return jobMasterIP + ":" + jobMasterPort;
    }

    // if the cache is not started, start it
    if (jobMasterNodeCache == null) {
      jobMasterNodeCache = new NodeCache(client, jobMasterPath);
      try {
        jobMasterNodeCache.start();
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Exception when starting jobMasterNodeCache", e);
      }
    }

    ChildData currentData = jobMasterNodeCache.getCurrentData();
    if (currentData == null) {
      return null;
    } else {
      String jobMasterIPandPort = new String(currentData.getData());
      setJobMasterIPandPort(jobMasterIPandPort);
      return jobMasterIPandPort;
    }
  }

  public void close() {
    client.close();
  }

}
